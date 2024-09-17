import dagster
import geopandas
import pandas as pd


def publish_metadata(
    context: dagster.AssetExecutionContext,
    pdf: pd.DataFrame | geopandas.GeoDataFrame,
    output_name: str = None,
) -> None:
    """Publishes metadata for pandas dataframes given a dagster execution context.
    Note excludes any column named "geometry" since these GIS data make reading tabular data
    difficult."""
    temp_df = pdf[[c for c in pdf.columns if c != "geometry"]]

    metadata = {
        "rows": len(temp_df),
        "preview": dagster.MetadataValue.md(temp_df.head().to_markdown()),
    }

    # If output_name is provided, use it; otherwise, log metadata without it
    if output_name:
        context.add_output_metadata(metadata, output_name=output_name)
    else:
        context.add_output_metadata(metadata)
