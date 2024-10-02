from collections import defaultdict
from itertools import chain
from typing import Dict, Tuple, Any

import dagster
import geopandas
import igraph as ig
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ireiat.config import INTERMEDIATE_DIRECTORY_ARGS, RR_MAPPING
from ireiat.data_pipeline.assets.rail_network.impedance import generate_impedance_graph
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.graph import (
    get_coordinates_from_geoframe,
    generate_zero_based_node_maps,
    get_allowed_node_indices,
)
from ireiat.util.rail_network_constants import EdgeType, VertexType

SEPARATION_ATTRIBUTE_NAME: str = "owners"  # field used to represent owners and trackage rights


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", "use_geopandas": True, **INTERMEDIATE_DIRECTORY_ARGS},
)
def filtered_and_processed_rail_network_links(
    context: dagster.AssetExecutionContext, narn_rail_network_links_src: geopandas.GeoDataFrame
) -> geopandas.GeoDataFrame:
    """Preprocess the rail links data by filtering records that are known to be defunct (e.g. NET=A, Abandoned)
    and consolidating the railroad owner(s) and railroad trackage rights into a single field"""
    # remove abandoned, physically removed, and out of service
    EXCLUDED_TRACK_NET_VALUES = ["A", "R", "X"]
    bad_track_filter = narn_rail_network_links_src["NET"].isin(EXCLUDED_TRACK_NET_VALUES)

    # Remove links that have AMTK as the owner with no other owner and all TRKRGHTS columns are null
    amtk_filter = (
        (narn_rail_network_links_src["RROWNER1"] == "AMTK")
        & narn_rail_network_links_src["RROWNER2"].isna()
        & narn_rail_network_links_src["RROWNER3"].isna()
    )
    for col in [c for c in narn_rail_network_links_src.columns if "TRKRGHTS" in c]:
        amtk_filter &= narn_rail_network_links_src[col].isna()

    real_lines = narn_rail_network_links_src[~amtk_filter & ~bad_track_filter].copy()
    ownership_cols = [col for col in real_lines.columns if "RROWNER" in col or "TRKRGHTS" in col]

    for col in ownership_cols:
        real_lines[col] = real_lines[col].replace(RR_MAPPING)

    owner_set = real_lines[ownership_cols].apply(lambda x: set(filter(pd.notna, x)), axis=1)
    # remove AMTK as a relevant owner, since interchange costs will not matter with AMTK
    owner_set.apply(lambda x: x.discard("AMTK"))
    # Add CSXT and NS to OWNERS if PAS is one of the owners (PAS is jointly owned by CSXT and NS)
    owner_set.apply(lambda x: x.update(["CSXT", "NS"]) if "PAS" in x else x)

    real_lines[SEPARATION_ATTRIBUTE_NAME] = owner_set
    real_lines["TRACKS"] = real_lines["TRACKS"].replace(0, 1)

    # columns to retain
    cols_to_retain = ["FRAARCID", "MILES", "TRACKS", SEPARATION_ATTRIBUTE_NAME, "geometry"]
    # exclude items with no owners at all
    real_lines_with_owners = real_lines.loc[
        real_lines[SEPARATION_ATTRIBUTE_NAME].apply(len) >= 1, cols_to_retain
    ].copy()
    context.log.info(
        f"Rail links data loaded and preprocessed with {len(real_lines_with_owners)} rail links"
    )
    publish_metadata(context, real_lines_with_owners)
    return real_lines_with_owners


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def undirected_rail_edges(
    context: dagster.AssetExecutionContext,
    filtered_and_processed_rail_network_links: geopandas.GeoDataFrame,
) -> pd.DataFrame:
    """For each undirected edge in the rail dataset, create a row in the table with origin_lat, origin_long,
    destination_lat, and destination_long, along with several other edge fields of interest"""

    link_coords = get_coordinates_from_geoframe(filtered_and_processed_rail_network_links)
    fields_to_retain = ["FRAARCID", SEPARATION_ATTRIBUTE_NAME, "MILES", "TRACKS"]
    link_coords = pd.concat(
        [filtered_and_processed_rail_network_links[fields_to_retain], link_coords], axis=1
    )  # join in the direction
    publish_metadata(context, link_coords)
    return link_coords


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def strongly_connected_rail_graph(
    context: dagster.AssetExecutionContext, undirected_rail_edges: pd.DataFrame
) -> ig.Graph:
    """iGraph object representing a strongly connected, directed graph based on the rail network"""
    # generate directed edges from the undirected edges based on the "dir" field
    edge_tuples = []
    edge_attributes = []
    undirected_rail_edges[SEPARATION_ATTRIBUTE_NAME] = undirected_rail_edges[
        SEPARATION_ATTRIBUTE_NAME
    ].apply(set)

    complete_rail_node_to_idx: Dict[Tuple[float, float], int] = generate_zero_based_node_maps(
        undirected_rail_edges
    )
    for row in undirected_rail_edges.itertuples():
        origin_coords = (row.origin_latitude, row.origin_longitude)
        destination_coords = (row.destination_latitude, row.destination_longitude)
        tail, head = (
            complete_rail_node_to_idx[origin_coords],
            complete_rail_node_to_idx[destination_coords],
        )

        # record some original edge information needed for visualization and/or TAP setup
        attribute_tuple = (
            row.MILES,
            row.FRAARCID,
            row.owners,
            origin_coords,
            destination_coords,
            row.TRACKS,
        )
        edge_tuples.append((tail, head))
        edge_attributes.append(attribute_tuple)
        edge_tuples.append((head, tail))
        edge_attributes.append(attribute_tuple)

    # generate a graph from all nodes
    n_vertices = len(complete_rail_node_to_idx)
    context.log.info(f"Original number of nodes {n_vertices}, edges {len(edge_tuples)}.")
    g = ig.Graph(
        n_vertices,
        edge_tuples,
        edge_attrs={
            "length": [attr[0] for attr in edge_attributes],
            "original_id": [attr[1] for attr in edge_attributes],
            "owners": [attr[2] for attr in edge_attributes],
            "edge_type": [EdgeType.RAIL_LINK.value for _ in edge_attributes],
            "origin_coords": [attr[3] for attr in edge_attributes],
            "destination_coords": [attr[4] for attr in edge_attributes],
            "tracks": [attr[5] for attr in edge_attributes],
        },
        directed=True,
    )
    context.log.info(f"Initial constructed graph connected?: {g.is_connected()}")
    allowed_node_indices = get_allowed_node_indices(g)

    # construct a connected subgraph
    connected_subgraph = g.subgraph(allowed_node_indices)
    context.log.info(f"Graph has {len(g.vs)} nodes and {len(g.es)} edges.")
    return connected_subgraph


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def impedance_rail_graph(
    context: dagster.AssetExecutionContext,
    strongly_connected_rail_graph: ig.Graph,
) -> ig.Graph:
    """iGraph object representing the impedance network, derived from the rail network"""
    g = generate_impedance_graph(strongly_connected_rail_graph, SEPARATION_ATTRIBUTE_NAME)
    context.log.info(f"Graph has {len(g.vs)} nodes and {len(g.es)} edges.")
    assert g.is_connected()
    return g


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def impedance_rail_graph_with_terminals(
    context: dagster.AssetExecutionContext,
    intermodal_terminals_src: pd.DataFrame,
    impedance_rail_graph: ig.Graph,
) -> ig.Graph:
    """Add intermodal facilities and attach them to the impedance rail graph at the right place.
    This asset matches owners at the intermodal facilities to the closest impedance graph node(s)"""

    # convert RAIL CO to a set, replacing things inside of parentheses and that occur with large strings ending in '-'
    intermodal_idx_to_rail_carriers = (
        intermodal_terminals_src["RAIL_CO"]
        .str.replace("via", ",")
        .str.replace(" ", "")
        .str.replace(r"\(.*\)", "", regex=True)
        .replace(r".*-", "", regex=True)
        .str.split(",")
        .apply(lambda x: set(x))
    )

    def replace_items(item_set):
        return {RR_MAPPING.get(item, item) for item in item_set}

    intermodal_terminals_src["RAIL_CO"] = intermodal_idx_to_rail_carriers.apply(replace_items)

    intersection_of_im_and_rail = intermodal_terminals_src["RAIL_CO"].apply(
        lambda x: x & set(impedance_rail_graph.es["owners"])
    )
    im_terminals_found_in_rail = intersection_of_im_and_rail.loc[
        intersection_of_im_and_rail != set()
    ]

    context.log.info(
        f"{len(im_terminals_found_in_rail)} IM terminals found in rail network out of {len(intermodal_terminals_src)}"
    )
    im_pdf = intermodal_terminals_src.iloc[im_terminals_found_in_rail.index].reset_index()

    # add vertices for the terminals and dummy nodes
    num_vertices_to_add = len(im_pdf) * 2
    vertex_attributes = defaultdict(list)
    for row in im_pdf.itertuples():
        for vertex_type in [VertexType.IM_TERMINAL, VertexType.IM_DUMMY_NODE]:
            vertex_attributes["terminal_idx"].append(row.Index)
            vertex_attributes["terminal_name"].append(row.TERMINAL)
            vertex_attributes["coords"].append((row.LAT, row.LON))
            vertex_attributes["vertex_type"].append(vertex_type.value)
            vertex_attributes["owners"].append(row.RAIL_CO)

    impedance_rail_graph.add_vertices(num_vertices_to_add, attributes=vertex_attributes)

    # get actual vertex indices for these just-added vertices
    terminal_idx_to_graph_vertex_idx_map = {
        v["terminal_idx"]: v.index
        for v in impedance_rail_graph.vs.select(vertex_type=VertexType.IM_TERMINAL)
    }
    dummy_terminal_idx_to_graph_vertex_idx_map = {
        v["terminal_idx"]: v.index
        for v in impedance_rail_graph.vs.select(vertex_type=VertexType.IM_DUMMY_NODE)
    }

    # create a map of (owner, origin coords)->source vertex index for all non-impedance edges
    owner_coord_to_vertex_idx_map = {
        (e["owners"], e["origin_coords"]): e.source_vertex.index
        for e in impedance_rail_graph.es
        if e["edge_type"] != EdgeType.IMPEDANCE_LINK.value
    }

    quant_node_lat_longs = [
        origin_coords for _, origin_coords in owner_coord_to_vertex_idx_map.keys()
    ]
    quant_lat_longs_radians = np.deg2rad(quant_node_lat_longs)
    quant_node_ball_tree = BallTree(quant_lat_longs_radians, metric="haversine")

    im_lat_longs = im_pdf[["LAT", "LON"]]
    im_fac_lat_longs_radians = np.deg2rad(im_lat_longs)

    # look up the closest 1000 nodes to a given terminal - if we find an operator match, great. if not, we discard
    distances_radians, lookup_to_bt_node_idx = quant_node_ball_tree.query(
        im_fac_lat_longs_radians, k=1000
    )
    edges = []
    edge_attributes: Dict[str, list[Any]] = defaultdict(list)
    for im_idx, record in enumerate(im_pdf.itertuples()):
        rrs_at_im: set = record.RAIL_CO
        dummy_terminal_node_vertex_idx = dummy_terminal_idx_to_graph_vertex_idx_map[im_idx]
        im_terminal_node_vertex_idx = terminal_idx_to_graph_vertex_idx_map[im_idx]

        # add edges between the IM terminal and its dummy node
        edges.append((im_terminal_node_vertex_idx, dummy_terminal_node_vertex_idx))
        edges.append((dummy_terminal_node_vertex_idx, im_terminal_node_vertex_idx))
        for i in range(2):
            edge_attributes["edge_type"].append(EdgeType.IM_CAPACITY.value)
            edge_attributes["length"].append(0.1)  # nominal length

        # now try to figure out where to map to the quant network
        for rr_at_im in rrs_at_im:
            for candidate_quant_node in lookup_to_bt_node_idx[im_idx]:
                # try to get a vertex with the particular owner and the matching lat long from the quant network
                matching_vertex = owner_coord_to_vertex_idx_map.get(
                    (rr_at_im, quant_node_lat_longs[candidate_quant_node])
                )
                if matching_vertex:
                    edges.append(
                        (dummy_terminal_idx_to_graph_vertex_idx_map[im_idx], matching_vertex)
                    )
                    edges.append(
                        (matching_vertex, dummy_terminal_idx_to_graph_vertex_idx_map[im_idx])
                    )
                    for i in range(2):
                        edge_attributes["length"].append(0.1)  # nominal length
                        edge_attributes["edge_type"].append(EdgeType.IM_DUMMY.value)
                    break
            else:
                context.log.info(
                    f"Nothing found for {rr_at_im} for terminal number {im_idx} with name"
                    f" {record.TERMINAL}"
                )

    impedance_rail_graph.add_edges(edges, attributes=edge_attributes)
    context.log.info(
        f"Graph has {len(impedance_rail_graph.vs)} nodes and {len(impedance_rail_graph.es)} edges."
    )
    assert impedance_rail_graph.is_connected()
    return impedance_rail_graph


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def impedance_rail_graph_with_terminals_reduced(
    context: dagster.AssetExecutionContext,
    impedance_rail_graph_with_terminals: ig.Graph,
) -> ig.Graph:
    """Compute IM to IM shortest paths and reduce the rail impedance network to only consider these edges"""
    im_indices = [
        v.index
        for v in impedance_rail_graph_with_terminals.vs.select(
            vertex_type=VertexType.IM_TERMINAL.value
        )
    ]

    # get all the edges for IM->IM on the impedance network
    edge_set: set[int] = set()
    for idx, im_from in enumerate(im_indices):
        destinations = [i for i in im_indices if i != im_from]
        edge_results = impedance_rail_graph_with_terminals.get_shortest_paths(
            im_from, destinations, weights="length", output="epath"
        )
        edge_set.update(chain.from_iterable(edge_results))

    # get the edges between IM->IM_dummy and IM_dummy->Impedance network
    im_capacity_edges = [
        e.index
        for e in impedance_rail_graph_with_terminals.es.select(edge_type=EdgeType.IM_CAPACITY)
    ]
    im_dummy_edges = [
        e.index for e in impedance_rail_graph_with_terminals.es.select(edge_type=EdgeType.IM_DUMMY)
    ]
    edge_set.update(im_capacity_edges)
    edge_set.update(im_dummy_edges)
    small_g = impedance_rail_graph_with_terminals.subgraph_edges(edge_set)
    context.log.info(f"Graph has {len(small_g.vs)} nodes and {len(small_g.es)} edges.")
    assert small_g.is_connected()
    return small_g
