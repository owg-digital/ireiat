from collections import Counter
from itertools import chain
from typing import Dict, Tuple

import dagster
import geopandas
import igraph as ig
import numpy as np
import pandas as pd

from ireiat.config import LATLONG_CRS, INTERMEDIATE_DIRECTORY_ARGS
from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def undirected_highway_edges(
    context: dagster.AssetExecutionContext, faf5_highway_network_links: geopandas.GeoDataFrame
) -> pd.DataFrame:
    """For each undirected edge in the highway dataset, create a row in the table with origin_lat, origin_long,
    destination_lat, and destination_long, along with several other edge fields of interest"""

    # convert to CRS suitable for extracting lat/long
    faf5_highway_network_links = faf5_highway_network_links.to_crs(LATLONG_CRS)
    coords = (
        faf5_highway_network_links.geometry.get_coordinates()
        .reset_index()
        .groupby("index")
        .agg({"x": ["first", "last"], "y": ["first", "last"]})
    )
    coords = coords.droplevel(0, axis=1)
    coords.columns = [
        "origin_longitude",
        "destination_longitude",
        "origin_latitude",
        "destination_latitude",
    ]
    coords = np.round(coords, 6)  # round to 6 decimal places of lat long
    assert len(coords) == len(faf5_highway_network_links)
    coords = pd.concat(
        [faf5_highway_network_links[["id", "dir", "length", "ab_finalsp"]], coords], axis=1
    )  # join in several fields of interest
    publish_metadata(context, coords)
    return coords


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_highway_node_to_idx(undirected_highway_edges: pd.DataFrame):
    """Generate unique nodes->indices based on the entire highway network"""
    unfiltered_node_idx_dict: Dict[Tuple[float, float], int] = dict()
    unfiltered_node_idx_counter = 0

    for row in undirected_highway_edges.itertuples():
        origin_coords = (row.origin_latitude, row.origin_longitude)
        destination_coords = (row.destination_latitude, row.destination_longitude)
        if origin_coords not in unfiltered_node_idx_dict:
            unfiltered_node_idx_dict[origin_coords] = unfiltered_node_idx_counter
            unfiltered_node_idx_counter += 1
        if destination_coords not in unfiltered_node_idx_dict:
            unfiltered_node_idx_dict[destination_coords] = unfiltered_node_idx_counter
            unfiltered_node_idx_counter += 1

    return unfiltered_node_idx_dict


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_highway_idx_to_node(complete_highway_node_to_idx):
    """Generates unique indices->nodes based on the entire highway network"""
    return {v: k for k, v in complete_highway_node_to_idx.items()}


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def strongly_connected_highway_graph(
    context: dagster.AssetExecutionContext,
    undirected_highway_edges: pd.DataFrame,
    complete_highway_node_to_idx: Dict[Tuple[float, float], int],
) -> ig.Graph:
    """iGraph object representing a strongly connected, directed graph based on the highway network"""
    # generate directed edges from the undirected edges based on the "dir" field
    edge_tuples = []
    edge_attributes = []

    for row in undirected_highway_edges.itertuples():
        origin_coords = (row.origin_latitude, row.origin_longitude)
        destination_coords = (row.destination_latitude, row.destination_longitude)
        tail, head = (
            complete_highway_node_to_idx[origin_coords],
            complete_highway_node_to_idx[destination_coords],
        )

        # record some original edge information needed for visualization and/or TAP setup
        attribute_tuple = (row.length, row.ab_finalsp, row.id)
        if row.dir == 1:  # A-> B only
            edge_tuples.append((tail, head))
            edge_attributes.append(attribute_tuple)
        elif row.dir == -1:  # B->A only
            edge_tuples.append((head, tail))  # check these!
            edge_attributes.append(attribute_tuple)
        else:
            edge_tuples.append((tail, head))
            edge_attributes.append(attribute_tuple)
            edge_tuples.append((head, tail))
            edge_attributes.append(attribute_tuple)

    # generate a graph from all nodes
    n_vertices = len(complete_highway_node_to_idx)
    context.log.info(f"Original number of nodes {n_vertices}, edges {len(edge_tuples)}.")
    g = ig.Graph(
        n_vertices,
        edge_tuples,
        vertex_attrs={"original_node_idx": list(complete_highway_node_to_idx.values())},
        edge_attrs={
            "length": [attr[0] for attr in edge_attributes],
            "speed": [attr[1] for attr in edge_attributes],
            "original_id": [attr[2] for attr in edge_attributes],
        },
        directed=True,
    )
    context.log.info(f"Initial constructed graph connected?: {g.is_connected()}")

    # filter for the largest connected component (we are basically throwing away the weakly connected nodes)
    connected_component_length = Counter([len(f) for f in g.connected_components()])
    biggest_connected_component = max(connected_component_length)

    context.log.info(f"Number of nodes in largest connected component {connected_component_length}")
    count_of_excluded_nodes = sum(
        [k * v for k, v in connected_component_length.items() if k != biggest_connected_component]
    )
    context.log.info(
        f"Excluded # of nodes that are not strongly connected {count_of_excluded_nodes}"
    )
    allowed_node_indices = list(
        chain.from_iterable(
            [x for x in g.connected_components() if len(x) == biggest_connected_component]
        )
    )

    # construct a connected subgraph
    connected_subgraph = g.subgraph(allowed_node_indices)
    return connected_subgraph


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def highway_network_dataframe(
    context: dagster.AssetExecutionContext, strongly_connected_highway_graph: ig.Graph
) -> pd.DataFrame:
    """Returns a dataframe of graph edges along with attributes needed to solve the TAP"""
    connected_edge_tuples = [
        (e.source, e.target, e["length"], e["speed"]) for e in strongly_connected_highway_graph.es
    ]

    # create and return a dataframe
    pdf = pd.DataFrame(connected_edge_tuples, columns=["tail", "head", "length", "speed"])
    publish_metadata(context, pdf)
    return pdf
