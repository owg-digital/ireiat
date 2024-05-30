import dagster
import pandas as pd

from ireiat.config import HIGHWAY_BETA, HIGHWAY_ALPHA, HIGHWAY_CAPACITY_TONS
from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", "write_kwargs": dagster.MetadataValue.json({"index": False})},
)
def tap_network_dataframe(
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
    publish_metadata(context, tap_network)
    return tap_network
