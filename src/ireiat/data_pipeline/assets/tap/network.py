import dagster
import igraph as ig
import numpy as np
import pandas as pd

from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.config.rail_network_constants import EdgeType
from ireiat.config.constants import (
    INTERMEDIATE_DIRECTORY_ARGS,
    HIGHWAY_BETA,
    HIGHWAY_ALPHA,
    HIGHWAY_CAPACITY_TONS,
    RAIL_DEFAULT_MPH_SPEED,
    IM_CAPACITY_TONS,
    RAIL_CAPACITY_TONS_PER_TRACK,
    RAIL_DEFAULT_LINK_CAPACITY_TONS,
    RAIL_BETA_IM,
    RAIL_BETA,
    RAIL_ALPHA_IM,
    RAIL_ALPHA,
    MARINE_DEFAULT_MPH_SPEED,
    MARINE_BETA,
    MARINE_ALPHA,
    MARINE_CAPACITY_TONS,
)


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
    """Entire highway network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = highway_network_dataframe
    tap_network["speed"] = tap_network["speed"].fillna(
        tap_network["speed"].mean()
    )  # fill in any null speeds
    # replace any zero speeds with the mean speed
    tap_network["speed"] = tap_network["speed"].replace(0, tap_network["speed"].mean())
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
    context: dagster.AssetExecutionContext,
    rail_graph_with_county_connections: ig.Graph,
) -> pd.DataFrame:
    """Entire rail network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP

    connected_edge_tuples = [
        (e.source, e.target, e["length"], e["edge_type"], e["owners"], e["speed"], e["tracks"])
        for e in rail_graph_with_county_connections.es
    ]

    # create and return a dataframe
    tap_network = pd.DataFrame(
        connected_edge_tuples,
        columns=["tail", "head", "length", "edge_type", "owners", "speed", "tracks"],
    )

    tap_network["speed"] = tap_network["speed"].fillna(RAIL_DEFAULT_MPH_SPEED)
    tap_network["length"] = tap_network["length"].fillna(0.1)
    tap_network["fft"] = tap_network["length"] / tap_network["speed"]

    # Apply beta and alpha based on edge type
    is_im_capacity_edge = tap_network["edge_type"] == EdgeType.IM_CAPACITY.value
    is_rail_link_edge = tap_network["edge_type"] == EdgeType.RAIL_LINK.value

    tap_network["capacity"] = np.where(
        is_im_capacity_edge,
        IM_CAPACITY_TONS,
        np.where(
            is_rail_link_edge,
            tap_network["tracks"] * RAIL_CAPACITY_TONS_PER_TRACK,
            RAIL_DEFAULT_LINK_CAPACITY_TONS,
        ),
    )
    tap_network["beta"] = np.where(is_im_capacity_edge, RAIL_BETA_IM, RAIL_BETA)
    tap_network["alpha"] = np.where(is_im_capacity_edge, RAIL_ALPHA_IM, RAIL_ALPHA)

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
    """Entire marine network to represent the TAP, complete with capacity and cost information"""
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
