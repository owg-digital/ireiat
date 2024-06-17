from collections import defaultdict
from typing import Dict, Tuple

import dagster
import pandas as pd

from ireiat.config import SUM_TONS_TOLERANCE


@dagster.asset(
    io_manager_key="default_io_manager", description="(State FIPS, County FIPS) -> Metric"
)
def actual_state_county_to_metric_map(
    us_census_county_population_src: pd.DataFrame,
) -> Dict[Tuple[str, str], int]:
    result: Dict[Tuple[str, str], int] = dict()
    # ignore state total information
    county_only_data = us_census_county_population_src.loc[
        us_census_county_population_src["COUNTY"] != "000"
    ]
    for row in county_only_data.itertuples():
        result[(row.STATE, row.COUNTY)] = row.POPESTIMATE2022
    return result


@dagster.asset(
    io_manager_key="default_io_manager",
    description="FAF -> (State FIPS, County FIPS) -> % allocation",
)
def faf_id_to_county_id_allocation_map(
    faf_id_to_county_areas: Dict[str, Dict[Tuple[str, str], float]],
    actual_state_county_to_metric_map: Dict[Tuple[str, str], int],
):

    faf_id_to_county_id_metric_map: Dict[str, Dict[Tuple[str, str], float]] = defaultdict(dict)

    # look up the county population within each faf zone
    for faf_id, vals in faf_id_to_county_areas.items():
        for (state_id, county_id), pct_area_county_in_faf in vals.items():
            total_county_pop = actual_state_county_to_metric_map[(state_id, county_id)]
            faf_id_to_county_id_metric_map[faf_id][(state_id, county_id)] = (
                total_county_pop * pct_area_county_in_faf
            )

    faf_total_metric_map = dict()
    for faf_id, vals_dict in faf_id_to_county_id_metric_map.items():
        faf_total_metric_map[faf_id] = sum(vals_dict.values())

    # check that the totals between the metric and those allocated to counties are equal
    assert sum(actual_state_county_to_metric_map.values()) == sum(faf_total_metric_map.values())

    # determine the percentage of the faf demand that should be allocated to the county
    # based on the allocation metric of interest
    faf_id_to_county_percent_map: Dict[str, Dict[Tuple[str, str], float]] = defaultdict(dict)
    for faf_id, vals in faf_id_to_county_id_metric_map.items():
        for (state_id, county_id), population_portion in vals.items():
            result_percent = population_portion / faf_total_metric_map[faf_id]
            faf_id_to_county_percent_map[faf_id][(state_id, county_id)] = result_percent

    # confirm that all the faf ids have a "total" allocation that sums to 1 (within some tolerance)
    assert all(
        [
            abs(sum(metric_map.values()) - 1) < SUM_TONS_TOLERANCE
            for metric_map in faf_id_to_county_percent_map.values()
        ]
    )

    return faf_id_to_county_percent_map
