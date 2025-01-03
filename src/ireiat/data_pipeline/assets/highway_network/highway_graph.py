from typing import Dict, Tuple

import dagster
import geopandas
import igraph as ig
import pandas as pd

from ireiat.config.constants import INTERMEDIATE_DIRECTORY_ARGS
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.graph import (
    get_coordinates_from_geoframe,
    generate_zero_based_node_maps,
    get_allowed_node_indices,
    explode_multilinestrings,
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
    fields_to_retain = ["dir", "length", "ab_finalsp", "geometry"]
    faf5_highway_network_links_src.columns = [
        c.lower() for c in faf5_highway_network_links_src.columns
    ]
    exploded_faf = explode_multilinestrings(faf5_highway_network_links_src[fields_to_retain])
    coords = get_coordinates_from_geoframe(exploded_faf)
    coords = pd.concat([exploded_faf, coords], axis=1)  # join in several fields of interest
    coords = coords.drop_duplicates(
        subset=[
            "origin_latitude",
            "origin_latitude",
            "destination_latitude",
            "destination_longitude",
        ]
    ).reset_index(drop=True)
    publish_metadata(context, coords)
    return coords


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def strongly_connected_highway_graph(
    context: dagster.AssetExecutionContext, undirected_highway_edges: pd.DataFrame
) -> ig.Graph:
    """iGraph object representing a strongly connected, directed graph based on the highway network"""
    # generate directed edges from the undirected edges based on the "dir" field
    edge_tuples = []
    edge_attributes = []
    added_edges: dict[tuple, bool] = {}  # needed to avoid duplicates

    complete_highway_node_to_idx: Dict[Tuple[float, float], int] = generate_zero_based_node_maps(
        undirected_highway_edges
    )
    g = ig.Graph(directed=True)
    g.add_vertices(
        len(complete_highway_node_to_idx),
        attributes={"coords": list(complete_highway_node_to_idx.keys())},
    )

    for row in undirected_highway_edges.itertuples():
        origin_coords = (row.origin_latitude, row.origin_longitude)
        destination_coords = (row.destination_latitude, row.destination_longitude)
        tail, head = (
            complete_highway_node_to_idx[origin_coords],
            complete_highway_node_to_idx[destination_coords],
        )

        # record some original edge information needed for visualization and/or TAP setup
        ab_attribute_tuple = (
            row.length,
            row.ab_finalsp,
            row.Index,
            origin_coords,
            destination_coords,
        )
        ba_attribute_tuple = (
            row.length,
            row.ab_finalsp,
            row.Index,
            destination_coords,
            origin_coords,
        )
        if row.dir == 1:  # A-> B only
            if (tail, head) not in added_edges.keys():
                edge_tuples.append((tail, head))
                added_edges[(tail, head)] = True
                edge_attributes.append(ab_attribute_tuple)
        elif row.dir == -1:  # B->A only
            if (head, tail) not in added_edges.keys():
                edge_tuples.append((head, tail))  # check these!
                edge_attributes.append(ba_attribute_tuple)
                added_edges[(head, tail)] = True
        else:
            if (tail, head) not in added_edges.keys():
                edge_tuples.append((tail, head))
                edge_attributes.append(ab_attribute_tuple)
                added_edges[(tail, head)] = True
            if (head, tail) not in added_edges.keys():
                edge_tuples.append((head, tail))
                edge_attributes.append(ba_attribute_tuple)
                added_edges[(head, tail)] = True

    # generate a graph from all nodes
    n_vertices = len(complete_highway_node_to_idx)
    context.log.info(f"Original number of nodes {n_vertices}, edges {len(edge_tuples)}.")
    g.add_edges(
        edge_tuples,
        attributes={
            "length": [attr[0] for attr in edge_attributes],
            "speed": [attr[1] for attr in edge_attributes],
            "original_id": [attr[2] for attr in edge_attributes],
            "origin_coords": [attr[3] for attr in edge_attributes],
            "destination_coords": [attr[4] for attr in edge_attributes],
        },
    )
    context.log.info(f"Initial constructed graph connected?: {g.is_connected()}")
    allowed_node_indices = get_allowed_node_indices(g)

    # construct a connected subgraph
    connected_subgraph = g.subgraph(allowed_node_indices)
    return connected_subgraph
