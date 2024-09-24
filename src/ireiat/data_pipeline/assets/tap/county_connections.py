from typing import Dict, Tuple

import dagster
import geopandas
import numpy as np
from sklearn.neighbors import BallTree

from ireiat.config import EXCLUDED_FIPS_CODES_MAP, LATLONG_CRS, ALBERS_CRS, RADIUS_EARTH_MILES


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

    centroid_radians = np.deg2rad(np.array(np.array(list(county_fips_to_centroid.values()))))
    distances_radians, centroid_idx_to_hwy_node_idx = highway_ball_tree.query(centroid_radians, k=1)
    distances_miles = distances_radians.squeeze() * RADIUS_EARTH_MILES
    centroid_idx_to_hwy_node_idx = centroid_idx_to_hwy_node_idx.squeeze()  # make a 1-D array

    context.log.info(f"Total highway node indices: {len(centroid_idx_to_hwy_node_idx)}")
    context.log.info(f"Unique highway node indices: {len(set(centroid_idx_to_hwy_node_idx))}")
    context.log.info(
        "If these aren't the same, then some (state,county) combos point to the same highway "
        "node...which is okay"
    )
    context.log.info(f"Max distance a highway node is: {max(distances_miles)} miles")

    return {k: int(v) for k, v in zip(county_fips_to_centroid.keys(), centroid_idx_to_hwy_node_idx)}
