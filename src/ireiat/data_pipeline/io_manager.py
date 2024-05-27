from functools import singledispatchmethod
from pathlib import Path
from typing import Optional, Mapping, Callable

import dagster
import geopandas
import pandas as pd
import pyogrio

from ireiat.config import CACHE_PATH
from ireiat.util.http import download_file


class FileSerializationResolver:
    @singledispatchmethod
    @staticmethod
    def serialize(obj) -> Callable:
        raise NotImplementedError(f"Cannot read or write {obj}!")

    @serialize.register
    @staticmethod
    def _(obj: pd.DataFrame) -> Callable:
        return obj.to_csv

    @serialize.register
    @staticmethod
    def _(obj: geopandas.GeoDataFrame) -> Callable:
        return obj.to_parquet


def _get_read_function(format: str) -> Callable:
    format_mapping = {
        "csv": pd.read_csv,
        "zip": pyogrio.read_dataframe,
        "txt": pd.read_table,
        "xlsx": pd.read_excel,
    }

    if format in format_mapping:
        return format_mapping[format]
    else:
        raise NotImplementedError(f"Cannot read file of type {format}!")


class TabularDataLocalIOManager(dagster.ConfigurableIOManager):
    """Translates tabular data (csv, txt, xlsx, and shp files) on the local filesystem."""

    def _get_fs_path(self, asset_key: dagster.AssetKey, metadata: Optional[Mapping]) -> str:
        """Gets the filesystem path based on the asset key and/or metadata.
        If the full source_path `some_file.csv` is passed, then use that. Otherwise use
        the filename and the default path, which is the user directory
        """
        source_path = CACHE_PATH / metadata.get("source_path", "")

        fname = metadata.get("filename") or f"{asset_key.path[0]}.{metadata.get('format')}"
        read_path = Path(source_path) / fname
        return str(read_path.absolute())

    def handle_output(self, context, obj: pd.DataFrame | geopandas.GeoDataFrame):
        """This saves the dataframe according to the format implemented in FileSerializationResolver."""

        fpath = self._get_fs_path(context.asset_key, context.metadata)
        FileSerializationResolver.serialize(obj)(fpath)

    def load_input(self, context):
        """This reads a dataframe based on file ending and metadata"""
        fpath = self._get_fs_path(
            context.upstream_output.asset_key, context.upstream_output.metadata
        )
        if not Path(fpath).exists():
            context.log.info(f"File {fpath} does not exist! Attempting download...")
            url: Optional[dagster.UrlMetadataValue] = context.upstream_output.metadata.get(
                "dashboard_url"
            )
            if not url:
                raise IOError(f"No metadata url specified for {fpath}!")
            download_file(url.url, fpath)
        fmt = fpath.split(".")[-1]
        read_func = _get_read_function(fmt)
        read_kwargs: Optional[dagster.JsonMetadataValue] = context.upstream_output.metadata.get(
            "read_kwargs"
        )
        parsed_read_kwargs: dict = read_kwargs.data if read_kwargs else dict()
        return read_func(fpath, **parsed_read_kwargs)
