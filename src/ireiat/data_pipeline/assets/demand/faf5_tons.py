from collections import defaultdict
from typing import Dict, Tuple

import dagster
import pandas as pd

from ireiat.config import TOLERANCE
from ireiat.data_pipeline.metadata import publish_metadata

TARGET_FIELD = "tons_2022"


@dagster.asset(io_manager_key="custom_io_manager", metadata={"format": "parquet"})
def faf5_truck_demand(
    context: dagster.AssetExecutionContext, faf5_demand_src: pd.DataFrame
) -> pd.DataFrame:
    """FAF5 demand by truck"""

    is_by_truck = faf5_demand_src["dms_mode"] == 1  # by truck
    faf_truck_pdf = faf5_demand_src.loc[is_by_truck]
    total_road_tons_od_pdf = faf_truck_pdf.groupby(["dms_orig", "dms_dest"], as_index=False)[
        [TARGET_FIELD]
    ].sum()
    publish_metadata(context, total_road_tons_od_pdf)
    return total_road_tons_od_pdf


@dagster.asset(io_manager_key="custom_io_manager", metadata={"format": "parquet"})
def county_to_county_tons(
    context: dagster.AssetExecutionContext,
    faf5_truck_demand: pd.DataFrame,
    faf_id_to_county_id_allocation_map: Dict[str, Dict[Tuple[str, str], float]],
) -> pd.DataFrame:
    """(State FIPS origin, County FIPS origin), (State FIPS destination, County FIPS destination), tons"""
    county_od: Dict[Tuple[str, str, str, str], float] = defaultdict(float)
    for row in faf5_truck_demand.itertuples():
        constituent_orig_counties_map = faf_id_to_county_id_allocation_map[row.dms_orig]
        constituent_dest_counties_map = faf_id_to_county_id_allocation_map[row.dms_dest]
        for (state_orig, county_orig), pct_in_county_orig in constituent_orig_counties_map.items():
            for (
                state_dest,
                county_dest,
            ), pct_in_county_dest in constituent_dest_counties_map.items():
                tons = getattr(row, TARGET_FIELD) * pct_in_county_orig * pct_in_county_dest
                county_od[(state_orig, county_orig, state_dest, county_dest)] += tons

    assert abs(faf5_truck_demand[TARGET_FIELD].sum() - sum(county_od.values())) < TOLERANCE

    county_od_pdf = pd.DataFrame(
        [(*k, v) for k, v in county_od.items()],
        columns=["state_orig", "county_orig", "state_dest", "county_dest", "tons"],
    )
    non_zero_county_od_pdf = county_od_pdf.loc[county_od_pdf["tons"] > 0].sort_values("tons")
    publish_metadata(context, non_zero_county_od_pdf)
    return non_zero_county_od_pdf
