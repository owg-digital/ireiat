from typing import Dict, Tuple

import dagster
import geopandas
import igraph as ig
import pandas as pd

from ireiat.config import INTERMEDIATE_DIRECTORY_ARGS
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.graph import (
    get_coordinates_from_geoframe,
    generate_zero_based_node_maps,
    get_allowed_node_indices,
)


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def undirected_highway_edges(
    context: dagster.AssetExecutionContext, faf5_highway_network_links_src: geopandas.GeoDataFrame
) -> pd.DataFrame:
    """For each undirected edge in the highway dataset, create a row in the table with origin_lat, origin_long,
    destination_lat, and destination_long, along with several other edge fields of interest"""

    coords = get_coordinates_from_geoframe(faf5_highway_network_links_src)
    coords = pd.concat(
        [faf5_highway_network_links_src[["id", "dir", "length", "ab_finalsp"]], coords], axis=1
    )  # join in several fields of interest
    publish_metadata(context, coords)
    return coords


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_highway_node_to_idx(undirected_highway_edges: pd.DataFrame):
    """Generate unique nodes->indices based on the entire highway network"""
    return generate_zero_based_node_maps(undirected_highway_edges)


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
    allowed_node_indices = get_allowed_node_indices(g)

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
    context.log.info(f"Highway network dataframe created with {len(pdf)} edges.")
    publish_metadata(context, pdf)
    return pdf
