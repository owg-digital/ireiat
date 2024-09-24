import dagster
import pandas as pd

import ireiat.config as CONFIG
from ireiat.util.rail_network_constants import EdgeType
from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json({"index": False}),
        **CONFIG.INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_highway_network_dataframe(
    context: dagster.AssetExecutionContext, highway_network_dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Entire highway network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = highway_network_dataframe
    tap_network["speed"] = tap_network["speed"].fillna(
        tap_network["speed"].mean()
    )  # fill in any null speeds
    # replace any zero speeds with the mean speed
    tap_network["speed"] = tap_network["speed"].replace(0, tap_network["speed"].mean())
    tap_network["fft"] = tap_network["length"] / tap_network["speed"]
    tap_network["beta"] = CONFIG.HIGHWAY_BETA
    tap_network["alpha"] = CONFIG.HIGHWAY_ALPHA
    tap_network["capacity"] = CONFIG.HIGHWAY_CAPACITY_TONS
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
        **CONFIG.INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_rail_network_dataframe(
    context: dagster.AssetExecutionContext, rail_network_dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Entire rail network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = rail_network_dataframe

    if "speed" not in tap_network.columns:
        # If the 'speed' column doesn't exist
        tap_network["speed"] = CONFIG.RAIL_DEFAULT_MPH_SPEED
    else:
        # Fill missing values with the default value
        tap_network["speed"] = tap_network["speed"].fillna(CONFIG.RAIL_DEFAULT_MPH_SPEED)

    tap_network["fft"] = tap_network["length"] / tap_network["speed"]

    # Apply beta and alpha based on edge type
    tap_network["beta"] = tap_network["edge_type"].apply(
        lambda x: CONFIG.RAIL_BETA_IM if x == EdgeType.IM_CAPACITY.value else CONFIG.RAIL_BETA
    )

    tap_network["alpha"] = tap_network["edge_type"].apply(
        lambda x: CONFIG.RAIL_ALPHA_IM if x == EdgeType.IM_CAPACITY.value else CONFIG.RAIL_ALPHA
    )

    # TODO: Utilize "TRACKNUM" field from the railway dataset to base capacity on the actual number of tracks
    tap_network["capacity"] = tap_network["capacity"].fillna(
        CONFIG.RAIL_DEFAULT_LINK_CAPACITY_TONS
    )  # fill in any null capacity values with default value from config.py

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
        **CONFIG.INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_marine_network_dataframe(
    context: dagster.AssetExecutionContext, marine_network_dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Entire marine network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = marine_network_dataframe

    if "speed" not in tap_network.columns:
        # If the 'speed' column doesn't exist
        tap_network["speed"] = CONFIG.MARINE_DEFAULT_MPH_SPEED
    else:
        # Fill missing values with the default value
        tap_network["speed"] = tap_network["speed"].fillna(CONFIG.MARINE_DEFAULT_MPH_SPEED)

    tap_network["fft"] = tap_network["length"] / tap_network["speed"]
    tap_network["beta"] = CONFIG.MARINE_BETA
    tap_network["alpha"] = CONFIG.MARINE_ALPHA
    tap_network["capacity"] = CONFIG.MARINE_CAPACITY_TONS
    tap_network = tap_network.sort_values(["tail", "head"])

    assert tap_network["speed"].min() > 0
    context.log.info(f"TAP marine network dataframe created with {len(tap_network)} edges.")
    publish_metadata(context, tap_network)
    return tap_network
