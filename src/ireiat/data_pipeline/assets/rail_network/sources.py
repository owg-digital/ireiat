import dagster

from ireiat.data_pipeline.io_manager import asset_spec_factory

narn_links_spec = dagster.AssetSpec(
    key=dagster.AssetKey("narn_rail_network_links_spec"),
    description="Publicly available GIS data for railway network links",
    metadata={
        "format": "zip",
        "filename": "narn_rail_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://hub.arcgis.com/api/download/v1/items/e143f436d4774402aa8cca1e663b1d24/shapefile?redirect=true&layers=0"
        ),
    },
)
narn_links_src = asset_spec_factory(narn_links_spec)

intermodal_terminals_spec = dagster.AssetSpec(
    key=dagster.AssetKey("intermodal_terminals_spec"),
    description="XLSX containing intermodal terminal information",
    metadata={
        "format": "xlsx",
        "filename": "im_terminals.xlsx",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://hub.arcgis.com/api/download/v1/items/e6332597fcd546d3899f60ec9b9157b9/excel?redirect=true&layers=0"
        ),
    },
)
intermodal_terminals_src = asset_spec_factory(intermodal_terminals_spec)
