from collections import defaultdict
from copy import deepcopy
from typing import Dict, Tuple, List

import dagster
import geopandas
import numpy as np


@dagster.asset(io_manager_key="default_io_manager")
def faf_id_to_county_areas(
    context: dagster.AssetExecutionContext,
    faf5_regions_src: geopandas.GeoDataFrame,
    us_county_shp_files_src: geopandas.GeoDataFrame,
) -> Dict[str, Dict[Tuple[str, str], float]]:
    """Reads FAF5 GIS data, county GIS data, and computes a FAF zone -> (County FIPS, State FIPS) -> % area map"""
    # area computation requires consistent CRS representation
    county_gdf = us_county_shp_files_src.to_crs(faf5_regions_src.crs)
    county_gdf["area"] = county_gdf.area

    # county shp file data is unique at STATEFP, COUNTYFP composite key
    assert len(county_gdf[["STATEFP", "COUNTYFP"]].drop_duplicates()) == len(county_gdf)

    # construct a map of this dataframe's index to (STATEFP,COUNTYFP)
    county_idx_map = {row.Index: (row.STATEFP, row.COUNTYFP) for row in county_gdf.itertuples()}

    # Compute which counties are part of which FAF5 region (and what percentage of their area)
    INTERSECTION_AREA_TOLERANCE_PERCENT = 0.01
    faf_zone_to_county = dict()
    for row in faf5_regions_src.itertuples():
        # return just the "overlap" of the FAF5 region with county data
        county_intersection = county_gdf.intersection(row.geometry)
        nonzero_county_intersection = county_intersection[~county_intersection.is_empty]

        # get the full county data and compute the percentage of county overlap by area,
        # ignoring anything less than the tolerance
        full_intersected_counties = county_gdf.loc[nonzero_county_intersection.index]
        intersected_counties_relative_area = (
            nonzero_county_intersection.area / full_intersected_counties.area
        )
        tol_adjusted_county_intersection = intersected_counties_relative_area.loc[
            np.where(
                intersected_counties_relative_area > INTERSECTION_AREA_TOLERANCE_PERCENT,
                True,
                False,
            )
        ]

        faf_zone_to_county[row.faf_zone] = np.round(tol_adjusted_county_intersection, 2).to_dict()

    # compute a map that links faf_zone_ids to counties
    # compute a map that tabulates the percentage of a county's area that's covered by a FAF zone
    county_coverage: Dict[Tuple[str, str], float] = defaultdict(float)
    county_id_to_faf_zone_id: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for faf_zone_id, val in faf_zone_to_county.items():
        for county_id, coverage in val.items():
            county_coverage[county_id] += coverage
            county_id_to_faf_zone_id[county_id].append(faf_zone_id)

    # get the county keys that are not totally covered
    not_totally_covered_county_ids = [
        k for k, covered_area_pct in county_coverage.items() if covered_area_pct < 1
    ]

    # if a county's area has not been totally covered AND there's more than one FAF zone that covers this county...we have a problem
    assert {
        k: len(v)
        for k, v in county_id_to_faf_zone_id.items()
        if k in not_totally_covered_county_ids and len(v) > 1
    } == dict()

    # as long as there is only 1 FAF region covering a "problem" county, we assign *all* of the county's area to be fully part of the FAF region
    faf_zone_to_county_canonical = deepcopy(faf_zone_to_county)
    for faf_zone, county_dict in faf_zone_to_county_canonical.items():
        for some_uncovered_county_id in not_totally_covered_county_ids:
            if some_uncovered_county_id in county_dict.keys():
                county_dict[some_uncovered_county_id] = 1.0

    county_coverage_canonical: Dict[Tuple[str, str], float] = defaultdict(float)
    for faf_zone_id, val in faf_zone_to_county_canonical.items():
        for county_id, coverage in val.items():
            county_coverage_canonical[county_id] += coverage

    # ensure that our canonical linkage of FAF zone IDs to county areas is complete
    assert min(county_coverage_canonical.values()) == 1.0

    # ensure every faf zone is covered
    assert (set(faf5_regions_src["faf_zone"]) - set(faf_zone_to_county_canonical.keys())) == set()

    # we relied on the index of the county geographic data to identify each county row - but, for usage with census data,
    # we serialize using the (STATEFP, COUNTYFP) code
    faf_zone_id_to_state_county_id = dict()
    for faf_zone_id, county_data in faf_zone_to_county_canonical.items():
        faf_zone_id_to_state_county_id[faf_zone_id] = {
            county_idx_map.get(k): v for k, v in county_data.items()
        }

    # bubble up metadata associated with this result
    context.add_output_metadata({"records": len(faf_zone_id_to_state_county_id.keys())})
    return faf_zone_id_to_state_county_id
