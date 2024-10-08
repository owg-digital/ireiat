from typing import Dict, Tuple

import dagster
import pandas as pd

from ireiat.config.constants import (
    SUM_TONS_TOLERANCE,
    INTERMEDIATE_DIRECTORY_ARGS,
)
from ireiat.config.data_pipeline import DataPipelineConfig
from ireiat.config.faf_enum import FAFMode
from ireiat.data_pipeline.assets.demand.faf5_helpers import (
    faf5_compute_county_tons_for_mode,
)
from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def faf_filtered_grouped_tons(
    context: dagster.AssetExecutionContext,
    faf5_demand_src: pd.DataFrame,
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Filters FAF by containerizable SCTG2 codes and relevant modes, multiplies by containerizable
    demand in each record, and groups by origin/destination/mode."""
    containerizable_codes = [
        x.sctg2 for x in config.demand_config.faf_commodities if x.containerizable
    ]
    context.log.info(f"Using {len(containerizable_codes)} containerizable codes")
    is_containerizable = faf5_demand_src["sctg2"].isin(containerizable_codes)
    is_relevant_mode = faf5_demand_src["dms_mode"].isin(
        [FAFMode.TRUCK, FAFMode.RAIL, FAFMode.WATER, FAFMode.MULTIPLE_AND_MAIL]
    )
    filtered_faf_pdf = faf5_demand_src.loc[is_containerizable & is_relevant_mode]

    grouped_faf_pdf = filtered_faf_pdf.groupby(
        ["dms_orig", "dms_dest", "dms_mode"], as_index=False
    )[[config.faf_demand_field]].sum()
    publish_metadata(context, grouped_faf_pdf)
    return grouped_faf_pdf


def _allocate_unknown_modes(
    context: dagster.AssetExecutionContext,
    entire_df: pd.DataFrame,
    specific_mode: int,
    percentage_unknown_to_specific: float,
    demand_field: str,
) -> pd.DataFrame:
    known_mode = entire_df.loc[entire_df["dms_mode"] == specific_mode]
    unknown_mode = entire_df.loc[entire_df["dms_mode"] == FAFMode.MULTIPLE_AND_MAIL.value]
    unknown_mode[demand_field] = unknown_mode[demand_field] * percentage_unknown_to_specific
    concat_pdf = pd.concat([known_mode, unknown_mode], axis=0)
    mode_pdf = concat_pdf.groupby(["dms_orig", "dms_dest"], as_index=False)[demand_field].sum()
    publish_metadata(context, mode_pdf)
    return mode_pdf


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def faf5_truck_demand(
    context: dagster.AssetExecutionContext,
    faf_filtered_grouped_tons: pd.DataFrame,
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Aggregates FAF truck mode and relevant portion of unknown mode into a single dataframe,
    grouped by origin and destination, summing total tons"""
    return _allocate_unknown_modes(
        context,
        faf_filtered_grouped_tons,
        FAFMode.TRUCK.value,
        config.demand_config.unknown_mode_percent_truck,
        config.faf_demand_field,
    )


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def faf5_rail_demand(
    context: dagster.AssetExecutionContext,
    faf_filtered_grouped_tons: pd.DataFrame,
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Aggregates FAF rail mode and relevant portion of unknown mode into a single dataframe,
    grouped by origin and destination, summing total tons"""
    return _allocate_unknown_modes(
        context,
        faf_filtered_grouped_tons,
        FAFMode.RAIL.value,
        config.demand_config.unknown_mode_percent_rail,
        config.faf_demand_field,
    )


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def faf5_water_demand(
    context: dagster.AssetExecutionContext,
    faf_filtered_grouped_tons: pd.DataFrame,
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Aggregates FAF marine mode and relevant portion of unknown mode into a single dataframe,
    grouped by origin and destination, summing total tons"""
    return _allocate_unknown_modes(
        context,
        faf_filtered_grouped_tons,
        FAFMode.WATER.value,
        config.demand_config.unknown_mode_percent_marine,
        config.faf_demand_field,
    )


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def county_to_county_highway_tons(
    context: dagster.AssetExecutionContext,
    faf5_truck_demand: pd.DataFrame,
    faf_id_to_county_id_allocation_map: Dict[str, Dict[Tuple[str, str], float]],
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Calculate (State FIPS origin, County FIPS origin), (State FIPS destination, County FIPS destination), tons
    for given mode based on county allocation percentages."""
    non_zero_county_od_pdf = faf5_compute_county_tons_for_mode(
        faf5_truck_demand,
        faf_id_to_county_id_allocation_map,
        config.faf_demand_field,
        SUM_TONS_TOLERANCE,
    )
    publish_metadata(context, non_zero_county_od_pdf)
    return non_zero_county_od_pdf


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def county_to_county_rail_tons(
    context: dagster.AssetExecutionContext,
    faf5_rail_demand: pd.DataFrame,
    faf_id_to_county_id_allocation_map: Dict[str, Dict[Tuple[str, str], float]],
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Calculate (State FIPS origin, County FIPS origin), (State FIPS destination, County FIPS destination), tons
    for given mode based on county allocation percentages."""
    non_zero_county_od_pdf = faf5_compute_county_tons_for_mode(
        faf5_rail_demand,
        faf_id_to_county_id_allocation_map,
        config.faf_demand_field,
        SUM_TONS_TOLERANCE,
    )
    publish_metadata(context, non_zero_county_od_pdf)
    return non_zero_county_od_pdf


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def county_to_county_marine_tons(
    context: dagster.AssetExecutionContext,
    faf5_water_demand: pd.DataFrame,
    faf_id_to_county_id_allocation_map: Dict[str, Dict[Tuple[str, str], float]],
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Calculate (State FIPS origin, County FIPS origin), (State FIPS destination, County FIPS destination), tons
    for given mode based on county allocation percentages."""
    non_zero_county_od_pdf = faf5_compute_county_tons_for_mode(
        faf5_water_demand,
        faf_id_to_county_id_allocation_map,
        config.faf_demand_field,
        SUM_TONS_TOLERANCE,
    )
    publish_metadata(context, non_zero_county_od_pdf)
    return non_zero_county_od_pdf
