from typing import Dict, Tuple

import dagster
import geopandas
import numpy as np
from sklearn.neighbors import BallTree

from ireiat.config import EXCLUDED_FIPS_CODES_MAP, LATLONG_CRS, ALBERS_CRS, RADIUS_EARTH_MILES


@dagster.asset(io_manager_key="default_io_manager")
def county_fips_to_highway_network_node_idx(
    context: dagster.AssetExecutionContext,
    us_county_shp_files_src: geopandas.GeoDataFrame,
    highway_ball_tree: BallTree,
) -> Dict[Tuple[str, str], int]:
    """For relevant counties in the US county GIS data, maps county centroids to node indices in the highway
    network. E.g. (STATE_FIPS, COUNTY_FIPS) -> N"""
    # exclude states that we don't care about
    county_gdf = us_county_shp_files_src.loc[
        ~(us_county_shp_files_src["STATEFP"].isin(EXCLUDED_FIPS_CODES_MAP.values()))
    ]
    county_centroids = county_gdf.to_crs(ALBERS_CRS).geometry.centroid.to_crs(LATLONG_CRS)

    # create mapping: (STATE FIPS, COUNTY FIPS) -> county centroid
    # create mapping: county centroid idx -> (STATE FIPS, COUNTY FIPS)...needed for the Ball Tree
    county_centroid_dict: Dict[Tuple[str, str], Tuple[float, float]] = dict()
    county_centroid_idx_dict: Dict[int, Tuple[str, str]] = dict()
    for idx, (row, centroid) in enumerate(zip(county_gdf.itertuples(), county_centroids)):
        key = (row.STATEFP, row.COUNTYFP)
        county_centroid_dict[key] = (round(centroid.y, 6), round(centroid.x, 6))
        county_centroid_idx_dict[idx] = key

    centroid_radians = np.deg2rad(np.array(np.array(list(county_centroid_dict.values()))))
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

    return {
        county_centroid_idx_dict[idx]: int(v) for idx, v in enumerate(centroid_idx_to_hwy_node_idx)
    }
