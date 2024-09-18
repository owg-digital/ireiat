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
from ireiat.data_pipeline.assets.demand.faf5_helpers import faf5_process_mode
from ireiat.util.faf_constants import FAFMode


@dagster.multi_asset(
    outs={
        "faf5_truck_demand": dagster.AssetOut(
            io_manager_key="custom_io_manager",
            metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
        ),
        "faf5_rail_demand": dagster.AssetOut(
            io_manager_key="custom_io_manager",
            metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
        ),
        "faf5_water_demand": dagster.AssetOut(
            io_manager_key="custom_io_manager",
            metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
        ),
    }
)
def faf5_demand_by_mode(
    context: dagster.AssetExecutionContext, faf5_demand_src: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Multi-asset function to extract demand data for different transport modes (Truck, Rail, Water).
    Rows with unknown modes are distributed proportionally based on known modes for each origin-destination pair.
    """
    # Filter for containerizable commodities
    is_containerizable = ~faf5_demand_src["sctg2"].isin(NON_CONTAINERIZABLE_COMMODITIES)
    faf_containerizable_pdf = faf5_demand_src.loc[is_containerizable]

    # Split into known modes (truck, rail, water) and unknown modes
    known_modes_pdf = faf_containerizable_pdf[
        faf_containerizable_pdf["dms_mode"].isin([FAFMode.TRUCK, FAFMode.RAIL, FAFMode.WATER])
    ]
    unknown_modes_pdf = faf_containerizable_pdf[
        faf_containerizable_pdf["dms_mode"] == FAFMode.OTHER_AND_UNKNOWN
    ]

    # Aggregate by origin/destination for known modes
    known_agg_pdf = known_modes_pdf.groupby(["dms_orig", "dms_dest", "dms_mode"], as_index=False)[
        [FAF_TONS_TARGET_FIELD]
    ].sum()

    # Pivot by origin/destination to calculate percentages for each mode
    mode_pivot_pdf = known_agg_pdf.pivot_table(
        index=["dms_orig", "dms_dest"],
        columns="dms_mode",
        values=FAF_TONS_TARGET_FIELD,
        fill_value=0,
    )

    # Add total tons and calculate percentages
    mode_pivot_pdf["total_tons"] = mode_pivot_pdf.sum(axis=1)
    mode_pivot_pdf["truck_pct"] = mode_pivot_pdf[FAFMode.TRUCK] / mode_pivot_pdf["total_tons"]
    mode_pivot_pdf["rail_pct"] = mode_pivot_pdf[FAFMode.RAIL] / mode_pivot_pdf["total_tons"]
    mode_pivot_pdf["water_pct"] = mode_pivot_pdf[FAFMode.WATER] / mode_pivot_pdf["total_tons"]

    # Calculate unique origin-destination pairs in unknown modes before filtering
    original_unknown_pairs = unknown_modes_pdf[["dms_orig", "dms_dest"]].drop_duplicates()

    # Perform an inner merge to filter valid origin-destination pairs and keep only common rows
    common_modes_pdf = unknown_modes_pdf.merge(
        mode_pivot_pdf[["truck_pct", "rail_pct", "water_pct"]],
        on=["dms_orig", "dms_dest"],
        how="inner",
    )

    # Calculate the set of valid origin-destination pairs
    valid_orig_dest_pairs = set(
        common_modes_pdf[["dms_orig", "dms_dest"]].drop_duplicates().apply(tuple, axis=1)
    )

    # Get the set difference between original unknown origin-destination pairs and valid pairs
    invalid_orig_dest_pairs = (
        set(original_unknown_pairs.apply(tuple, axis=1)) - valid_orig_dest_pairs
    )

    # Log a warning about the dropped origin-destination pairs
    dropped_pairs_count = len(invalid_orig_dest_pairs)
    if dropped_pairs_count > 0:
        context.log.warning(
            f"{dropped_pairs_count} unique dms origin-destination pairs were dropped because they don't exist in the known (truck, rail, water) modes."
        )

    # Process and re-aggregate unknown mode rows for truck, rail, and water based on their respective proportions
    truck_pdf = faf5_process_mode(
        common_modes_pdf,
        known_modes_pdf,
        FAFMode.TRUCK.value,
        "truck_tons",
        "truck_pct",
        FAF_TONS_TARGET_FIELD,
    )
    rail_pdf = faf5_process_mode(
        common_modes_pdf,
        known_modes_pdf,
        FAFMode.RAIL.value,
        "rail_tons",
        "rail_pct",
        FAF_TONS_TARGET_FIELD,
    )
    water_pdf = faf5_process_mode(
        common_modes_pdf,
        known_modes_pdf,
        FAFMode.WATER.value,
        "water_tons",
        "water_pct",
        FAF_TONS_TARGET_FIELD,
    )

    # Log and store the dataframes
    context.log.info(f"Truck mode demand generated with {len(truck_pdf)} records.")
    context.log.info(f"Rail mode demand generated with {len(rail_pdf)} records.")
    context.log.info(f"Water mode demand generated with {len(water_pdf)} records.")

    publish_metadata(context, truck_pdf, output_name="faf5_truck_demand")
    publish_metadata(context, rail_pdf, output_name="faf5_rail_demand")
    publish_metadata(context, water_pdf, output_name="faf5_water_demand")

    # Return the dataframes in the same order as the hardcoded outs
    return truck_pdf, rail_pdf, water_pdf


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
