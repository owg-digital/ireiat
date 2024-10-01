import tempfile
from functools import partial
from pathlib import Path
from typing import Optional, Mapping, Callable
from zipfile import ZipFile

import dagster
import geopandas
import pandas as pd
import pyogrio

from ireiat.config import CACHE_PATH
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.http import download_uncached_file


def _get_read_function(format: str, metadata: dict = None) -> Callable:
    """Returns a function for file-reading based on the `format` and possibly `metadata`"""
    format_mapping = {
        "csv": pd.read_csv,
        "parquet": (
            geopandas.read_parquet
            if metadata and metadata.get("use_geopandas", False)
            else pd.read_parquet
        ),
        "zip": partial(pyogrio.read_dataframe, use_arrow=True),
        "txt": pd.read_table,
        "xlsx": pd.read_excel,
    }
    if metadata and metadata.get("temp_unzip", False):
        return _temp_unzip_and_read
    if format in format_mapping:
        print(format_mapping)
        return format_mapping[format]
    else:
        raise NotImplementedError(f"Cannot read file of type {format}!")


def _temp_unzip_and_read(fpath, **kwargs):
    """Unzips the zipfile passed at fpath and reads the first csv into a pandas dataframe"""
    zip = ZipFile(fpath)
    with tempfile.TemporaryDirectory() as tmpdirname:
        zip.extractall(path=tmpdirname)
        target_csvs = [f for f in zip.namelist() if f.endswith("csv")]
        pdf = pd.read_csv(zip.open(target_csvs[0]), **kwargs)
        zip.close()
    return pdf


def _get_fs_path(asset_key: dagster.AssetKey, metadata: Optional[Mapping]) -> str:
    """Gets the filesystem path based on the asset key and/or metadata.
    If the full source_path `some_file.csv` is passed, then use that. Otherwise use
    the filename and the default path, which is the user directory
    """
    source_path = CACHE_PATH / metadata.get("source_path", "")
    fname = metadata.get("filename") or f"{asset_key.path[0]}.{metadata.get('format')}"
    read_path = Path(source_path) / fname
    return str(read_path.absolute())


def read_or_attempt_download(
    asset_key: dagster.AssetKey, current_asset_metadata
) -> pd.DataFrame | geopandas.GeoDataFrame:
    """Reads the file given the metadata. If the file does not exist, attempts to download based on url information
    within the metadata. If download info does not exist and the file is not in the filesystem, throws an IOError
    """
    fpath = _get_fs_path(asset_key, current_asset_metadata)

    # if URL specified, attempt download...
    url: Optional[dagster.UrlMetadataValue] = current_asset_metadata.get("dashboard_url")
    if url:
        download_uncached_file(url.url, fpath)

    if not Path(fpath).exists():
        raise IOError(f"No metadata url specified for {fpath}!")

    read_kwargs: Optional[dagster.JsonMetadataValue] = current_asset_metadata.get("read_kwargs")
    parsed_read_kwargs: dict = read_kwargs.data if read_kwargs else dict()

    # figure out how to read this based on the ending
    fmt = fpath.split(".")[-1]
    read_func = _get_read_function(fmt, current_asset_metadata)

    return read_func(fpath, **parsed_read_kwargs)


class TabularDataLocalIOManager(dagster.ConfigurableIOManager):
    """Translates tabular data (csv, txt, xlsx, and shp files) on the local filesystem."""

    def handle_output(self, context, obj: pd.DataFrame | geopandas.GeoDataFrame) -> None:
        """This saves the dataframe according to the format implemented in FileSerializationResolver."""

        fpath = _get_fs_path(context.asset_key, context.metadata)
        Path(fpath).parent.mkdir(exist_ok=True)

        write_kwargs: Optional[dagster.JsonMetadataValue] = context.metadata.get("write_kwargs")
        parsed_write_kwargs: dict = write_kwargs.data if write_kwargs else dict()
        fmt = fpath.split(".")[-1]
        if fmt == "parquet":
            obj.to_parquet(fpath, **parsed_write_kwargs)
        elif fmt == "csv":
            obj.to_csv(fpath, **parsed_write_kwargs)
        elif fmt == "xlsx":
            obj.to_excel(fpath, **parsed_write_kwargs)
        elif fmt == "zip":
            context.log.info("Assuming zip file from download. Skipping persistence.")
        else:
            raise NotImplementedError(f"Cannot write file of type {fmt}!")

    def load_input(self, context) -> pd.DataFrame | geopandas.GeoDataFrame:
        """This reads a dataframe based on file ending and metadata"""
        return read_or_attempt_download(
            context.upstream_output.asset_key, context.upstream_output.metadata
        )


def asset_spec_factory(spec: dagster.AssetSpec):
    """Creates a materialized asset from an asset spec, including potentially downloading it.
    The method relies on a naming convention for asset specs that end in "_spec" and generates
    assets that end in "_src"."""

    @dagster.asset(
        key=spec.key.to_user_string().replace("_spec", "_src"),
        io_manager_key="custom_io_manager",
        metadata=spec.metadata,
        description=spec.description,
    )
    def _asset(context: dagster.AssetExecutionContext):
        result = read_or_attempt_download(spec.key, spec.metadata)

        # TODO: Make adjustments downstream to expect lower case headers, then uncomment below:
        # Convert all column names to lowercase to avoid case-sensitivity issues
        # result = result.rename(columns=lambda x: x.lower())

        publish_metadata(context, result)
        return result

    return _asset
