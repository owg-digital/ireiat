from collections import defaultdict
from typing import Dict, Tuple

import dagster
import geopandas
import igraph as ig
import numpy as np
from sklearn.neighbors import BallTree

from ireiat.config.constants import (
    EXCLUDED_FIPS_CODES_MAP,
    LATLONG_CRS,
    ALBERS_CRS,
    RADIUS_EARTH_MILES,
    IM_SEARCH_RADIUS_MILES,
    HIGHWAY_DEFAULT_MPH_SPEED,
    HIGHWAY_DRAYAGE_PENALTY,
)
from ireiat.config.rail_network_constants import EdgeType, VertexType


def _generate_network_indices_from_ball_tree(
    context: dagster.AssetExecutionContext,
    county_centroids: Dict[Tuple[str, str], Tuple[float, float]],
    bt: BallTree,
) -> Dict[Tuple[str, str], int]:
    """Helper function for assets"""
    centroid_radians = np.deg2rad(np.array(np.array(list(county_centroids.values()))))
    distances_radians, centroid_idx_to_network_node_idx = bt.query(centroid_radians, k=1)
    distances_miles = distances_radians.squeeze() * RADIUS_EARTH_MILES
    centroid_idx_to_network_node_idx = (
        centroid_idx_to_network_node_idx.squeeze()
    )  # make a 1-D array

    context.log.info(f"Total node indices: {len(centroid_idx_to_network_node_idx)}")
    context.log.info(f"Unique node indices: {len(set(centroid_idx_to_network_node_idx))}")
    context.log.info(
        "If these aren't the same, then (state,county) combos point to the same network node, fyi"
    )
    context.log.info(f"Max distance a node is: {max(distances_miles)} miles")

    return {k: int(v) for k, v in zip(county_centroids.keys(), centroid_idx_to_network_node_idx)}


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def county_fips_to_centroid(
    us_county_shp_files_src: geopandas.GeoDataFrame,
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    """Returns (STATE_FIPS, COUNTY_FIPS) -> (latitude, longitude) based on county centroids"""
    # exclude states that we don't care about
    county_gdf = us_county_shp_files_src.loc[
        ~(us_county_shp_files_src["STATEFP"].isin(EXCLUDED_FIPS_CODES_MAP.values()))
    ]
    county_centroids = county_gdf.to_crs(ALBERS_CRS).geometry.centroid.to_crs(LATLONG_CRS)

    # create mapping: (STATE FIPS, COUNTY FIPS) -> county centroid
    county_centroid_dict: Dict[Tuple[str, str], Tuple[float, float]] = dict()
    for idx, (row, centroid) in enumerate(zip(county_gdf.itertuples(), county_centroids)):
        key = (row.STATEFP, row.COUNTYFP)
        county_centroid_dict[key] = (round(centroid.y, 6), round(centroid.x, 6))

    return county_centroid_dict


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def county_fips_to_highway_network_node_idx(
    context: dagster.AssetExecutionContext,
    county_fips_to_centroid: Dict[Tuple[str, str], Tuple[float, float]],
    highway_ball_tree: BallTree,
) -> Dict[Tuple[str, str], int]:
    """Map all county centroids to the nearest highway nodes returning a dict of (STATE, COUNTY) -> highway node"""
    return _generate_network_indices_from_ball_tree(
        context, county_fips_to_centroid, highway_ball_tree
    )


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def county_fips_to_marine_network_node_idx(
    context: dagster.AssetExecutionContext,
    county_fips_to_centroid: Dict[Tuple[str, str], Tuple[float, float]],
    marine_ball_tree: BallTree,
) -> Dict[Tuple[str, str], int]:
    """Map all county centroids to the nearest marine nodes returning a dict of (STATE, COUNTY) -> marine node"""
    return _generate_network_indices_from_ball_tree(
        context, county_fips_to_centroid, marine_ball_tree
    )


@dagster.multi_asset(
    outs={
        "rail_graph_with_county_connections": dagster.AssetOut(
            io_manager_key="default_io_manager_intermediate_path"
        ),
        "county_fips_to_rail_network_node_idx": dagster.AssetOut(
            io_manager_key="default_io_manager_intermediate_path"
        ),
    }
)
def rail_county_association(
    context: dagster.AssetExecutionContext,
    impedance_rail_graph_with_terminals_reduced: ig.Graph,
    county_fips_to_centroid: Dict[Tuple[str, str], Tuple[float, float]],
) -> Tuple[ig.Graph, Dict[Tuple[str, str], int]]:
    """Add county centroids to rail impedance network with county centroids connected to nearby IM facilities."""

    # unfortunately, there are several counties that may not have ANY intermodal facilities within a radius
    # we don't want to add these counties as vertices to the graph, so we need to filter them out first
    # by interrogating which IM facilities are nearby ALL counties
    g = impedance_rail_graph_with_terminals_reduced
    im_lat_longs = [v["coords"] for v in g.vs.select(vertex_type=VertexType.IM_TERMINAL.value)]
    im_lat_longs_radians = np.deg2rad(np.array(im_lat_longs))
    im_node_ball_tree = BallTree(im_lat_longs_radians, metric="haversine")

    search_radius_radians = IM_SEARCH_RADIUS_MILES / RADIUS_EARTH_MILES
    county_lat_longs = list(county_fips_to_centroid.values())
    reverse_county_lookup = {v: k for k, v in county_fips_to_centroid.items()}
    search_coords_radians = np.deg2rad(np.array(county_lat_longs))
    imfac_idxs, distances_radians = im_node_ball_tree.query_radius(
        search_coords_radians, r=search_radius_radians, return_distance=True, sort_results=True
    )
    county_fips_to_centroid_with_im = dict()
    for idx, candidate_ims in enumerate(imfac_idxs):
        if len(candidate_ims) > 0:
            key = reverse_county_lookup[county_lat_longs[idx]]
            county_fips_to_centroid_with_im[key] = county_fips_to_centroid[key]

    # log the results
    counties_without_im = len(county_fips_to_centroid) - len(county_fips_to_centroid_with_im)
    context.log.info(
        f"{counties_without_im} counties do not have IM facilities nearby. Excluding them from the graph."
    )

    # now we have a list of county centroids with IM facilities, which we add to the graph
    attr_dict = {
        "coords": [v for v in county_fips_to_centroid_with_im.values()],
        "vertex_type": [VertexType.COUNTY_CENTROID for _ in county_fips_to_centroid_with_im],
    }
    g.add_vertices(len(county_fips_to_centroid_with_im), attributes=attr_dict)

    # look up the coordinates and of each centroid vertex and map to the vertex ID (after adding centroid vertices)
    county_centroid_coords_to_vertex_idx = {
        v["coords"]: v.index for v in g.vs.select(vertex_type=VertexType.COUNTY_CENTROID.value)
    }

    # look up the coordinates and of each IM vertex and map to the vertex ID (after adding centroid vertices)
    im_coords_to_vertex_idx = {
        v["coords"]: v.index for v in g.vs.select(vertex_type=VertexType.IM_TERMINAL.value)
    }

    # create a ball tree
    im_node_lat_longs = list(im_coords_to_vertex_idx.keys())
    im_lat_longs_radians = np.deg2rad(np.array(im_node_lat_longs))
    im_node_ball_tree = BallTree(im_lat_longs_radians, metric="haversine")

    search_radius_radians = IM_SEARCH_RADIUS_MILES / RADIUS_EARTH_MILES
    county_lat_longs = list(county_centroid_coords_to_vertex_idx.keys())
    search_coords_radians = np.deg2rad(np.array(county_lat_longs))
    imfac_idxs, distances_radians = im_node_ball_tree.query_radius(
        search_coords_radians, r=search_radius_radians, return_distance=True, sort_results=True
    )
    distances_miles = distances_radians * RADIUS_EARTH_MILES

    edges_to_add = []
    edge_attributes = defaultdict(list)
    for idx, (candidate_ims, distances) in enumerate(zip(imfac_idxs, distances_miles)):
        for candidate_im, distance in zip(candidate_ims, distances):
            im_vertex = im_coords_to_vertex_idx[im_node_lat_longs[candidate_im]]
            county_vertex = county_centroid_coords_to_vertex_idx[county_lat_longs[idx]]
            edges_to_add.append((county_vertex, im_vertex))
            edges_to_add.append((im_vertex, county_vertex))
            for _ in range(2):
                edge_attributes["length"].append(distance)
                edge_attributes["edge_type"].append(EdgeType.COUNTY_TO_IM_LINK.value)
                edge_attributes["speed"].append(HIGHWAY_DEFAULT_MPH_SPEED / HIGHWAY_DRAYAGE_PENALTY)

    g.add_edges(edges_to_add, attributes=edge_attributes)

    assert g.is_connected()

    county_fips_to_rail_network_node_idx = {
        k: county_centroid_coords_to_vertex_idx[v]
        for k, v in county_fips_to_centroid_with_im.items()
    }
    return g, county_fips_to_rail_network_node_idx
