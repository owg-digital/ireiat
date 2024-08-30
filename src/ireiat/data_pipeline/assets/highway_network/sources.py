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
            "https://opendata.arcgis.com/api/v3/datasets/cbfd7a1457d749ae865f9212c978c645_0/downloads/data?format=shp&spatialRefId=3857&where=1%3D1"
        ),
    },
)
faf5_regions_src = asset_spec_factory(faf5_regions_spec)
