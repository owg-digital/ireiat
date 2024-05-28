import dagster
import geopandas
import pandas as pd


def publish_metadata(
    context: dagster.AssetExecutionContext, pdf: pd.DataFrame | geopandas.GeoDataFrame
) -> None:
    """Publishes metadata for pandas dataframes given a dagster execution context"""
    context.add_output_metadata(
        {"rows": len(pdf), "preview": dagster.MetadataValue.md(pdf.head().to_markdown())}
    )
