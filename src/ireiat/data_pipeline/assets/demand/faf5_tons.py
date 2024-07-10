from collections import defaultdict
from typing import Dict, Tuple

import dagster
import pandas as pd

from ireiat.config import (
    SUM_TONS_TOLERANCE,
    FAF_TONS_TARGET_FIELD,
    NON_CONTAINERIZABLE_COMMODITIES,
    INTERMEDIATE_DIRECTORY_ARGS,
)
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.faf_constants import FAFMode


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def faf5_truck_demand(
    context: dagster.AssetExecutionContext, faf5_demand_src: pd.DataFrame
) -> pd.DataFrame:
    """FAF5 containerizable demand using the truck mode. Currently, FAF containerizable demand
    is limited by commodity. Based on OW knowledge of containerizable commodities"""

    # filter for containerizable
    is_containerizable = ~faf5_demand_src["sctg2"].isin(NON_CONTAINERIZABLE_COMMODITIES)
    faf_containerizable_pdf = faf5_demand_src.loc[is_containerizable]

    # filter for truck mode
    is_by_truck = faf_containerizable_pdf["dms_mode"] == FAFMode.TRUCK
    faf_truck_pdf = faf_containerizable_pdf.loc[is_by_truck]

    total_road_tons_od_pdf = faf_truck_pdf.groupby(["dms_orig", "dms_dest"], as_index=False)[
        [FAF_TONS_TARGET_FIELD]
    ].sum()
    publish_metadata(context, total_road_tons_od_pdf)
    return total_road_tons_od_pdf


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def county_to_county_highway_tons(
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
                tons = getattr(row, FAF_TONS_TARGET_FIELD) * pct_in_county_orig * pct_in_county_dest
                county_od[(state_orig, county_orig, state_dest, county_dest)] += tons

    assert (
        abs(faf5_truck_demand[FAF_TONS_TARGET_FIELD].sum() - sum(county_od.values()))
        < SUM_TONS_TOLERANCE
    )

    county_od_pdf = pd.DataFrame(
        [(*k, v) for k, v in county_od.items()],
        columns=["state_orig", "county_orig", "state_dest", "county_dest", "tons"],
    )
    non_zero_county_od_pdf = county_od_pdf.loc[county_od_pdf["tons"] > 0].sort_values("tons")
    publish_metadata(context, non_zero_county_od_pdf)
    return non_zero_county_od_pdf
