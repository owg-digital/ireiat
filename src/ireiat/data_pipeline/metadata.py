from typing import Optional

import dagster
import geopandas
import pandas as pd

from ireiat.data_pipeline import TabularDataLocalIOManager
from ireiat.data_pipeline.io_manager import _get_read_function


def publish_metadata(
    context: dagster.AssetExecutionContext, pdf: pd.DataFrame | geopandas.GeoDataFrame
) -> None:
    """Publishes metadata for pandas dataframes given a dagster execution context"""
    context.add_output_metadata(
        {"rows": len(pdf), "preview": dagster.MetadataValue.md(pdf.head().to_markdown())}
    )


def observation_function(context: dagster.OpExecutionContext):
    """Passed to create AssetObservations for `dagster.SourceAsset` declarations.
    See https://docs.dagster.io/concepts/assets/asset-observations#attaching-metadata-to-an-assetobservation
    """
    current_asset_metadata = context.job_def.asset_layer.get(context.asset_key).metadata

    # generic read
    fpath = TabularDataLocalIOManager._get_fs_path(context.asset_key, current_asset_metadata)
    read_func = _get_read_function(fpath.split(".")[-1], current_asset_metadata)
    read_kwargs: Optional[dagster.JsonMetadataValue] = current_asset_metadata.get("read_kwargs")
    parsed_read_kwargs: dict = read_kwargs.data if read_kwargs else dict()

    # load the data and exclude any geometry columns, which don't play nice with Dagster's UI
    temp_df = read_func(fpath, **parsed_read_kwargs)
    temp_df = temp_df[[c for c in temp_df.columns if c != "geometry"]]

    context.log_event(
        dagster.AssetObservation(
            asset_key=context.asset_key,
            metadata={
                "rows": len(temp_df),
                "preview": dagster.MetadataValue.md(temp_df.head().to_markdown()),
            },
        )
    )

    return dagster.DataVersion("from_src")
