from itertools import chain
from typing import Set, Dict, Tuple

import dagster
import geopandas
import igraph as ig
import pandas as pd

from ireiat.config import INTERMEDIATE_DIRECTORY_ARGS, RAIL_DEFAULT_MPH_SPEED
from ireiat.data_pipeline.assets.rail_network.impedance import generate_impedance_graph
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.graph import (
    get_coordinates_from_geoframe,
    generate_zero_based_node_maps,
    get_allowed_node_indices,
)

SEPARATION_ATTRIBUTE_NAME: str = "owners"  # field used to represent owners and trackage rights


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", "use_geopandas": True, **INTERMEDIATE_DIRECTORY_ARGS},
)
def filtered_and_processed_rail_network_links(
    context: dagster.AssetExecutionContext, narn_rail_network_links: geopandas.GeoDataFrame
) -> geopandas.GeoDataFrame:
    """Preprocess the rail links data by filtering records that are known to be defunct (e.g. NET=A, Abandoned)
    and consolidating the railroad owner(s) and railroad trackage rights into a single field"""
    # remove abandoned, physically removed, and out of service
    EXCLUDED_TRACK_NET_VALUES = ["A", "R", "X"]
    bad_track_filter = narn_rail_network_links["NET"].isin(EXCLUDED_TRACK_NET_VALUES)

    # Remove links that have AMTK as the owner with no other owner and all TRKRGHTS columns are null
    amtk_filter = (
        (narn_rail_network_links["RROWNER1"] == "AMTK")
        & narn_rail_network_links["RROWNER2"].isna()
        & narn_rail_network_links["RROWNER3"].isna()
    )
    for col in [c for c in narn_rail_network_links.columns if "TRKRGHTS" in c]:
        amtk_filter &= narn_rail_network_links[col].isna()

    real_lines = narn_rail_network_links[~amtk_filter & ~bad_track_filter].copy()
    ownership_cols = [col for col in real_lines.columns if "RROWNER" in col or "TRKRGHTS" in col]
    rr_mapping_dict = {"CPRS": "CPKC", "KCS": "CPKC", "KCSM": "CPKC"}
    for col in ownership_cols:
        real_lines[col] = real_lines[col].replace(rr_mapping_dict)

    owner_set = real_lines[ownership_cols].apply(lambda x: set(filter(pd.notna, x)), axis=1)
    # remove AMTK as a relevant owner since we won't
    owner_set.apply(lambda x: x.discard("AMTK"))
    # Add CSXT and NS to OWNERS if PAS is one of the owners (PAS is jointly owned by CSXT and NS)
    owner_set.apply(lambda x: x.update(["CSXT", "NS"]) if "PAS" in x else x)

    real_lines[SEPARATION_ATTRIBUTE_NAME] = owner_set

    # columns to retain
    cols_to_retain = ["FRAARCID", "MILES", SEPARATION_ATTRIBUTE_NAME, "geometry"]
    # exclude items with now owners
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
    metadata={"format": "parquet", "use_geopandas": True, **INTERMEDIATE_DIRECTORY_ARGS},
)
def owner_rationalized_rail_links(
    context: dagster.AssetExecutionContext,
    filtered_and_processed_rail_network_links: geopandas.GeoDataFrame,
) -> geopandas.GeoDataFrame:
    """When constructing the impedance graph, there are 900+ rail owners/operators - but catering
    for the top N will shrink the size of the impedance graph while still catering for major interchanges.
    Currently, the number of owners considered is 40, which caters covers interchanges for 80% of the edges in the
    rail network"""
    COUNT_UNIQUE_ATTRIBUTE_VALUES = 40
    unique_owners_across_edges = set(
        chain.from_iterable(
            filtered_and_processed_rail_network_links[SEPARATION_ATTRIBUTE_NAME].values
        )
    )
    edge_list_by_owner: Dict[str, Set[int]] = {k: set() for k in unique_owners_across_edges}
    for row in filtered_and_processed_rail_network_links.itertuples():
        for k in getattr(row, SEPARATION_ATTRIBUTE_NAME):
            edge_list_by_owner[k].add(row.Index)

    owner_edge_count = [(k, len(v)) for k, v in edge_list_by_owner.items()]
    sorted_owner_edge_count = sorted(owner_edge_count, key=lambda x: x[1], reverse=True)
    top_owners_and_edge_counts = sorted_owner_edge_count[:COUNT_UNIQUE_ATTRIBUTE_VALUES]
    percent = sum([x[1] for x in top_owners_and_edge_counts]) / sum(
        [x[1] for x in owner_edge_count]
    )
    context.log.info(
        f"Filtering for {COUNT_UNIQUE_ATTRIBUTE_VALUES}, results in {percent:.1%} edges covered"
    )

    top_owners: Set[str] = set([x[0] for x in top_owners_and_edge_counts])
    context.log.info(f"Top owners {top_owners}")

    def shrink_attribute(current_row_attr_vals: Set):
        final_attr_vals = top_owners & current_row_attr_vals
        if len(current_row_attr_vals - top_owners) >= 1:
            final_attr_vals.add("Other")
        return final_attr_vals

    filtered_and_processed_rail_network_links[
        SEPARATION_ATTRIBUTE_NAME
    ] = filtered_and_processed_rail_network_links[SEPARATION_ATTRIBUTE_NAME].apply(
        set
    )  # when saving parquet, stores as a list
    shrunk_owners = filtered_and_processed_rail_network_links[SEPARATION_ATTRIBUTE_NAME].apply(
        shrink_attribute
    )
    filtered_and_processed_rail_network_links[SEPARATION_ATTRIBUTE_NAME] = shrunk_owners
    return filtered_and_processed_rail_network_links


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def undirected_rail_edges(
    context: dagster.AssetExecutionContext,
    owner_rationalized_rail_links: geopandas.GeoDataFrame,
) -> pd.DataFrame:
    """For each undirected edge in the rail dataset, create a row in the table with origin_lat, origin_long,
    destination_lat, and destination_long, along with several other edge fields of interest"""

    link_coords = get_coordinates_from_geoframe(owner_rationalized_rail_links)
    fields_to_retain = ["FRAARCID", SEPARATION_ATTRIBUTE_NAME, "MILES"]
    link_coords = pd.concat(
        [owner_rationalized_rail_links[fields_to_retain], link_coords], axis=1
    )  # join in the direction
    publish_metadata(context, link_coords)
    return link_coords


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_rail_node_to_idx(undirected_rail_edges: pd.DataFrame):
    """Generate unique nodes->indices based on the entire rail network"""
    return generate_zero_based_node_maps(undirected_rail_edges)


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_rail_idx_to_node(complete_rail_node_to_idx):
    """Generates unique indices->nodes based on the entire rail network"""
    return {v: k for k, v in complete_rail_node_to_idx.items()}


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def strongly_connected_rail_graph(
    context: dagster.AssetExecutionContext,
    undirected_rail_edges: pd.DataFrame,
    complete_rail_node_to_idx: Dict[Tuple[float, float], int],
) -> ig.Graph:
    """iGraph object representing a strongly connected, directed graph based on the rail network"""
    # generate directed edges from the undirected edges based on the "dir" field
    edge_tuples = []
    edge_attributes = []
    undirected_rail_edges[SEPARATION_ATTRIBUTE_NAME] = undirected_rail_edges[
        SEPARATION_ATTRIBUTE_NAME
    ].apply(set)
    for row in undirected_rail_edges.itertuples():
        origin_coords = (row.origin_latitude, row.origin_longitude)
        destination_coords = (row.destination_latitude, row.destination_longitude)
        tail, head = (
            complete_rail_node_to_idx[origin_coords],
            complete_rail_node_to_idx[destination_coords],
        )

        # record some original edge information needed for visualization and/or TAP setup
        # TODO (NP): Account for capacity  on the rail network
        attribute_tuple = (row.MILES, row.FRAARCID, row.owners)
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
        vertex_attrs={"original_node_idx": list(complete_rail_node_to_idx.values())},
        edge_attrs={
            "length": [attr[0] for attr in edge_attributes],
            "owners": [attr[2] for attr in edge_attributes],
            "speed": [RAIL_DEFAULT_MPH_SPEED for attr in edge_attributes],
            "original_id": [attr[1] for attr in edge_attributes],
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


# TODO (NP) - figure out how to attach intermodal terminals and the interplay with the impedances
# @dagster.asset(
#     io_manager_key="custom_io_manager",
#     metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
# )
# def rail_network_terminals(
#         context: dagster.AssetExecutionContext, intermodal_terminals: pd.DataFrame
# ) -> pd.DataFrame:
#     """Preprocess the intermodal terminals data"""
#     processed_terminals = data_handler.intermodal_terminals_preprocessing(intermodal_terminals)
#     context.log.info(
#         f"Intermodal terminals data loaded and preprocessed with {processed_terminals.shape[0]} terminals"
#     )
#     publish_metadata(context, processed_terminals)
#     return processed_terminals
