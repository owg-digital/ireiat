import dagster

from ireiat.data_pipeline.io_manager import asset_spec_factory

faf5_regions_spec = dagster.AssetSpec(
    key=dagster.AssetKey("faf5_highway_network_links_spec"),
    description="Publicly available GIS data for FAF5 highway network links",
    metadata={
        "format": "zip",
        "filename": "faf5_highway_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://hub.arcgis.com/api/download/v1/items/b33d6a096bb94ca6b5cf5d96bf68a682/shapefile?redirect=true&layers=0"
        ),
    },
)
faf5_regions_src = asset_spec_factory(faf5_regions_spec)
