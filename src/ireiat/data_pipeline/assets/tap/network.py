import dagster
import pandas as pd

from ireiat.config import (
    HIGHWAY_BETA,
    HIGHWAY_ALPHA,
    HIGHWAY_CAPACITY_TONS,
    RAIL_BETA,
    RAIL_ALPHA,
    RAIL_CAPACITY_TONS,
    RAIL_DEFAULT_MPH_SPEED,
    MARINE_BETA,
    MARINE_ALPHA,
    MARINE_CAPACITY_TONS,
    MARINE_DEFAULT_MPH_SPEED,
    INTERMEDIATE_DIRECTORY_ARGS,
)
from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json({"index": False}),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_highway_network_dataframe(
    context: dagster.AssetExecutionContext, highway_network_dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Entire network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = highway_network_dataframe
    tap_network["speed"] = tap_network["speed"].fillna(
        tap_network["speed"].mean()
    )  # fill in any null speeds
    tap_network["fft"] = tap_network["length"] / tap_network["speed"]
    tap_network["beta"] = HIGHWAY_BETA
    tap_network["alpha"] = HIGHWAY_ALPHA
    tap_network["capacity"] = HIGHWAY_CAPACITY_TONS
    tap_network = tap_network.sort_values(["tail", "head"])

    assert tap_network["speed"].min() > 0
    context.log.info(f"TAP highway network dataframe created with {len(tap_network)} edges.")
    publish_metadata(context, tap_network)
    return tap_network


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json({"index": False}),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_rail_network_dataframe(
    context: dagster.AssetExecutionContext, rail_network_dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Entire network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = rail_network_dataframe

    if "speed" not in tap_network.columns:
        # If the 'speed' column doesn't exist
        tap_network["speed"] = RAIL_DEFAULT_MPH_SPEED
    else:
        # Fill missing values with the default value
        tap_network["speed"] = tap_network["speed"].fillna(RAIL_DEFAULT_MPH_SPEED)

    tap_network["fft"] = tap_network["length"] / tap_network["speed"]
    tap_network["beta"] = RAIL_BETA
    tap_network["alpha"] = RAIL_ALPHA
    tap_network["capacity"] = RAIL_CAPACITY_TONS
    tap_network = tap_network.sort_values(["tail", "head"])

    assert tap_network["speed"].min() > 0
    context.log.info(f"TAP rail network dataframe created with {len(tap_network)} edges.")
    publish_metadata(context, tap_network)
    return tap_network


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json({"index": False}),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_marine_network_dataframe(
    context: dagster.AssetExecutionContext, marine_network_dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Entire network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = marine_network_dataframe

    if "speed" not in tap_network.columns:
        # If the 'speed' column doesn't exist
        tap_network["speed"] = MARINE_DEFAULT_MPH_SPEED
    else:
        # Fill missing values with the default value
        tap_network["speed"] = tap_network["speed"].fillna(MARINE_DEFAULT_MPH_SPEED)

    tap_network["fft"] = tap_network["length"] / tap_network["speed"]
    tap_network["beta"] = MARINE_BETA
    tap_network["alpha"] = MARINE_ALPHA
    tap_network["capacity"] = MARINE_CAPACITY_TONS
    tap_network = tap_network.sort_values(["tail", "head"])

    assert tap_network["speed"].min() > 0
    context.log.info(f"TAP marine network dataframe created with {len(tap_network)} edges.")
    publish_metadata(context, tap_network)
    return tap_network
