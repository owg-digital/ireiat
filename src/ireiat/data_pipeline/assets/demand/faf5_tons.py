from typing import Dict, Tuple

import dagster
import pandas as pd

from ireiat.config.constants import (
    SUM_TONS_TOLERANCE,
    NON_CONTAINERIZABLE_COMMODITIES,
    INTERMEDIATE_DIRECTORY_ARGS,
)
from ireiat.config.data_pipeline import DataPipelineConfig
from ireiat.config.faf_enum import FAFMode
from ireiat.data_pipeline.assets.demand.faf5_helpers import (
    faf5_process_mode,
    faf5_compute_county_tons_for_mode,
)
from ireiat.data_pipeline.metadata import publish_metadata


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
    context: dagster.AssetExecutionContext,
    faf5_demand_src: pd.DataFrame,
    config: DataPipelineConfig,
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
        [config.faf_demand_field]
    ].sum()

    # Pivot by origin/destination to calculate percentages for each mode
    mode_pivot_pdf = known_agg_pdf.pivot_table(
        index=["dms_orig", "dms_dest"],
        columns="dms_mode",
        values=config.faf_demand_field,
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
        config.faf_demand_field,
    )
    rail_pdf = faf5_process_mode(
        common_modes_pdf,
        known_modes_pdf,
        FAFMode.RAIL.value,
        "rail_tons",
        "rail_pct",
        config.faf_demand_field,
    )
    water_pdf = faf5_process_mode(
        common_modes_pdf,
        known_modes_pdf,
        FAFMode.WATER.value,
        "water_tons",
        "water_pct",
        config.faf_demand_field,
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


@dagster.multi_asset(
    outs={
        "county_to_county_highway_tons": dagster.AssetOut(
            io_manager_key="custom_io_manager",
            metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
        ),
        "county_to_county_rail_tons": dagster.AssetOut(
            io_manager_key="custom_io_manager",
            metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
        ),
        "county_to_county_marine_tons": dagster.AssetOut(
            io_manager_key="custom_io_manager",
            metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
        ),
    }
)
def county_to_county_tons(
    context: dagster.AssetExecutionContext,
    faf5_truck_demand: pd.DataFrame,
    faf5_rail_demand: pd.DataFrame,
    faf5_water_demand: pd.DataFrame,
    faf_id_to_county_id_allocation_map: Dict[str, Dict[Tuple[str, str], float]],
    config: DataPipelineConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Multi-asset function to calculate (State FIPS origin, County FIPS origin), (State FIPS destination, County FIPS destination), tons
    for truck, rail, and water modes based on county allocation percentages.
    """

    # Process county tons for truck, rail, and water modes
    non_zero_truck_county_od_pdf = faf5_compute_county_tons_for_mode(
        faf5_truck_demand,
        faf_id_to_county_id_allocation_map,
        config.faf_demand_field,
        SUM_TONS_TOLERANCE,
        "Truck",
    )
    non_zero_rail_county_od_pdf = faf5_compute_county_tons_for_mode(
        faf5_rail_demand,
        faf_id_to_county_id_allocation_map,
        config.faf_demand_field,
        SUM_TONS_TOLERANCE,
        "Rail",
    )
    non_zero_water_county_od_pdf = faf5_compute_county_tons_for_mode(
        faf5_water_demand,
        faf_id_to_county_id_allocation_map,
        config.faf_demand_field,
        SUM_TONS_TOLERANCE,
        "Water",
    )

    # Log and store the dataframes
    context.log.info(
        f"Truck mode county-to-county demand generated with {len(non_zero_truck_county_od_pdf)} records."
    )
    context.log.info(
        f"Rail mode county-to-county demand generated with {len(non_zero_rail_county_od_pdf)} records."
    )
    context.log.info(
        f"Water mode county-to-county demand generated with {len(non_zero_water_county_od_pdf)} records."
    )

    publish_metadata(
        context, non_zero_truck_county_od_pdf, output_name="county_to_county_highway_tons"
    )
    publish_metadata(context, non_zero_rail_county_od_pdf, output_name="county_to_county_rail_tons")
    publish_metadata(
        context, non_zero_water_county_od_pdf, output_name="county_to_county_marine_tons"
    )

    # Return dataframes for each mode
    return non_zero_truck_county_od_pdf, non_zero_rail_county_od_pdf, non_zero_water_county_od_pdf
