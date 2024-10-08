import dagster
import igraph as ig
import numpy as np
import pandas as pd

from ireiat.config.constants import (
    INTERMEDIATE_DIRECTORY_ARGS,
)
from ireiat.config.data_pipeline import DataPipelineConfig
from ireiat.config.rail_enum import EdgeType
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
    context: dagster.AssetExecutionContext,
    highway_network_dataframe: pd.DataFrame,
    config: DataPipelineConfig,
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
    tap_network["beta"] = config.highway_config.highway_edge_beta
    tap_network["alpha"] = config.highway_config.highway_edge_alpha
    tap_network["capacity"] = config.highway_config.default_capacity_ktons
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
    config: DataPipelineConfig,
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

    tap_network["speed"] = tap_network["speed"].fillna(config.rail_config.default_speed_mph)
    tap_network["length"] = tap_network["length"].fillna(0.1)
    tap_network["fft"] = tap_network["length"] / tap_network["speed"]

    # Apply beta and alpha based on edge type
    is_im_capacity_edge = tap_network["edge_type"] == EdgeType.IM_CAPACITY.value
    is_rail_link_edge = tap_network["edge_type"] == EdgeType.RAIL_LINK.value

    tap_network["capacity"] = np.where(
        is_im_capacity_edge,
        config.rail_config.intermodal_facility_capacity_ktons,
        np.where(
            is_rail_link_edge,
            tap_network["tracks"] * config.rail_config.capacity_ktons_per_track,
            config.rail_config.capacity_ktons_default,
        ),
    )
    tap_network["beta"] = np.where(
        is_im_capacity_edge,
        config.rail_config.intermodal_edge_beta,
        config.rail_config.rail_network_beta,
    )
    tap_network["alpha"] = np.where(
        is_im_capacity_edge,
        config.rail_config.intermodal_edge_alpha,
        config.rail_config.rail_network_alpha,
    )

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
    context: dagster.AssetExecutionContext,
    marine_network_dataframe: pd.DataFrame,
    config: DataPipelineConfig,
) -> pd.DataFrame:
    """Entire marine network to represent the TAP, complete with capacity and cost information"""
    # fill out other fields needed for the TAP
    tap_network = marine_network_dataframe

    if "speed" not in tap_network.columns:
        # If the 'speed' column doesn't exist
        tap_network["speed"] = config.marine_config.default_speed_mph
    else:
        # Fill missing values with the default value
        tap_network["speed"] = tap_network["speed"].fillna(config.marine_config.default_speed_mph)

    tap_network["fft"] = tap_network["length"] / tap_network["speed"]
    tap_network["beta"] = config.marine_config.marine_edge_beta
    tap_network["alpha"] = config.marine_config.marine_edge_alpha
    tap_network["capacity"] = config.marine_config.default_capacity_ktons
    tap_network = tap_network.sort_values(["tail", "head"])

    assert tap_network["speed"].min() > 0
    context.log.info(f"TAP marine network dataframe created with {len(tap_network)} edges.")
    publish_metadata(context, tap_network)
    return tap_network
