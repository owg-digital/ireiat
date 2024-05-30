import dagster

from ireiat.data_pipeline.metadata import observation_function


faf5_regions_src = dagster.SourceAsset(
    key=dagster.AssetKey("faf5_highway_network_links"),
    observe_fn=observation_function,
    description="Publicly available GIS data for FAF5 highway network links",
    io_manager_key="custom_io_manager",
    metadata={
        "format": "zip",
        "filename": "faf5_highway_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://opendata.arcgis.com/api/v3/datasets/cbfd7a1457d749ae865f9212c978c645_0/downloads/data?format=shp&spatialRefId=3857&where=1%3D1"
        ),
    },
)
