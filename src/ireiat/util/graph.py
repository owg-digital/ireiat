from collections import Counter
from itertools import chain
from typing import Dict, Tuple, List

import geopandas
import igraph as ig
import numpy as np
import pandas as pd
from shapely.geometry import MultiLineString
from sklearn.neighbors import BallTree

from ireiat.config.constants import LATLONG_CRS, ALBERS_CRS, METERS_PER_MILE


# break out each multiline
def explode_multilinestrings(gdf: geopandas.GeoDataFrame):
    """If a dataframe contains shapely.MultiLineString geometries, return a record in the dataframe
    for each line with the multiline string. Additionally, recompute the `length` field in miles
    for each row"""
    multiline_locs, line_locs = [], []
    for row in gdf.itertuples():
        if isinstance(row.geometry, MultiLineString):
            multiline_locs.append(row.Index)
        else:
            line_locs.append(row.Index)
    if not multiline_locs:
        return gdf

    concatenated_df = pd.concat(
        [gdf.loc[multiline_locs].explode("geometry"), gdf.loc[line_locs]], axis=0, ignore_index=True
    )
    concatenated_df["length"] = concatenated_df.to_crs(ALBERS_CRS).geometry.length / METERS_PER_MILE
    return concatenated_df


def get_coordinates_from_geoframe(gdf: geopandas.GeoDataFrame) -> pd.DataFrame:
    """Given a GeoDataFrame of line segments, returns the origin and destination coordinates of each line segment
    in a pandas DataFrame. Returned fields are origin_latitude, origin_longitude, destination_latitude, destination_longitude.
    Will ensure that the CRS for lat/longs are equal to EPSG:4326"""
    if gdf.crs != LATLONG_CRS:
        gdf = gdf.to_crs(LATLONG_CRS)
    link_coords = (
        gdf.geometry.get_coordinates()
        .reset_index()
        .groupby("index")
        .agg({"x": ["first", "last"], "y": ["first", "last"]})
    )
    link_coords = link_coords.droplevel(0, axis=1)
    link_coords.columns = [
        "origin_longitude",
        "destination_longitude",
        "origin_latitude",
        "destination_latitude",
    ]
    link_coords = np.round(link_coords, 6)  # round to 6 decimal places of lat long
    assert len(link_coords) == len(gdf)
    return link_coords


def generate_zero_based_node_maps(link_coords: pd.DataFrame) -> Dict[Tuple[float, float], int]:
    """Returns a dictionary of (lat,long)->index based on each row in the dataframe.
    The final index in the dictionary represents the count-1 of the identified nodes.
    Note: Nodes are assumed to be able to be uniquely identified by lat/long but no checks are made on the
    precision of lat/long in the `link_coords` dataframe!"""

    latlong_node_idx_dict: Dict[Tuple[float, float], int] = dict()
    latlong_node_idx_counter = 0

    for row in link_coords.itertuples():
        origin_coords = (row.origin_latitude, row.origin_longitude)
        destination_coords = (row.destination_latitude, row.destination_longitude)
        if origin_coords not in latlong_node_idx_dict:
            latlong_node_idx_dict[origin_coords] = latlong_node_idx_counter
            latlong_node_idx_counter += 1
        if destination_coords not in latlong_node_idx_dict:
            latlong_node_idx_dict[destination_coords] = latlong_node_idx_counter
            latlong_node_idx_counter += 1
    return latlong_node_idx_dict


def get_allowed_node_indices(g: ig.Graph) -> List[int]:
    """Given a graph, checks its connectedness and returns the indices of the
    nodes of the largest connected component"""
    # filter for the largest connected component (we are basically throwing away the weakly connected nodes)
    connected_component_length = Counter([len(f) for f in g.connected_components()])
    biggest_connected_component = max(connected_component_length)

    print(f"Number of nodes in largest connected component {connected_component_length}")
    count_of_excluded_nodes = sum(
        [k * v for k, v in connected_component_length.items() if k != biggest_connected_component]
    )
    print(f"Excluded # of nodes that are not strongly connected {count_of_excluded_nodes}")
    allowed_node_indices = list(
        chain.from_iterable(
            [x for x in g.connected_components() if len(x) == biggest_connected_component]
        )
    )
    return allowed_node_indices


def generate_ball_tree(g: ig.Graph) -> BallTree:
    """Generates a ball tree from a graph assuming that vertices all have a 'coords' attribute"""
    # stack unique origins / destinations by lat/long
    vertex_coords = g.vs["coords"]
    node_lat_long_radians = np.deg2rad(np.array(vertex_coords))
    return BallTree(node_lat_long_radians, metric="haversine")
