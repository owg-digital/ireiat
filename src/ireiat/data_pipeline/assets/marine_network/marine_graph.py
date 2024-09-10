import dagster
import geopandas
import pandas as pd
import igraph as ig
from ireiat.config import INTERMEDIATE_DIRECTORY_ARGS, ALBERS_CRS, METERS_PER_MILE
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.graph import (
    get_coordinates_from_geoframe,
    generate_zero_based_node_maps,
    get_allowed_node_indices,
)
from typing import Dict, Tuple


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def undirected_marine_edges(
    context: dagster.AssetExecutionContext, marine_network_links_src: geopandas.GeoDataFrame
) -> pd.DataFrame:
    """For each undirected edge in the marine dataset, create a row in the table with origin_lat, origin_long,
    destination_lat, and destination_long, along with several other edge fields of interest"""

    link_coords = get_coordinates_from_geoframe(marine_network_links_src)
    marine_network_albers = marine_network_links_src.to_crs(ALBERS_CRS)
    distance_miles = marine_network_albers.geometry.length / METERS_PER_MILE
    link_coords = pd.concat([link_coords, pd.Series(distance_miles, name="distance_miles")], axis=1)
    publish_metadata(context, link_coords)
    return link_coords


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_marine_node_to_idx(undirected_marine_edges: pd.DataFrame):
    """Generate unique nodes->indices based on the entire marine network"""
    return generate_zero_based_node_maps(undirected_marine_edges)


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_marine_idx_to_node(complete_marine_node_to_idx):
    """Generates unique indices->nodes based on the entire marine network"""
    return {v: k for k, v in complete_marine_node_to_idx.items()}


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def strongly_connected_marine_graph(
    context: dagster.AssetExecutionContext,
    undirected_marine_edges: pd.DataFrame,
    complete_marine_node_to_idx: Dict[Tuple[float, float], int],
) -> ig.Graph:
    """iGraph object representing a strongly connected, undirected graph based on the marine network"""
    edge_tuples = []
    edge_attributes = []

    for idx, row in enumerate(undirected_marine_edges.itertuples()):
        origin_coords = (row.origin_latitude, row.origin_longitude)
        destination_coords = (row.destination_latitude, row.destination_longitude)
        tail, head = (
            complete_marine_node_to_idx[origin_coords],
            complete_marine_node_to_idx[destination_coords],
        )
        edge_tuples.append((tail, head))
        edge_attributes.append((row.distance_miles, idx))

    # generate a graph from all nodes
    n_vertices = len(complete_marine_node_to_idx)
    context.log.info(f"Original number of nodes {n_vertices}, edges {len(edge_tuples)}.")
    g = ig.Graph(
        n_vertices,
        edge_tuples,
        vertex_attrs={"original_node_idx": list(complete_marine_node_to_idx.values())},
        edge_attrs={
            "length": [attr[0] for attr in edge_attributes],
            "original_id": [attr[1] for attr in edge_attributes],
        },
        directed=False,
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
def marine_network_dataframe(
    context: dagster.AssetExecutionContext, strongly_connected_marine_graph: ig.Graph
) -> pd.DataFrame:
    """Returns a dataframe of graph edges along with attributes needed to solve the TAP"""
    connected_edge_tuples = [
        (e.source, e.target, e["length"]) for e in strongly_connected_marine_graph.es
    ]

    # create and return a dataframe
    pdf = pd.DataFrame(connected_edge_tuples, columns=["tail", "head", "length"])
    publish_metadata(context, pdf)
    return pdf
