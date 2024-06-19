import dagster

from ireiat.data_pipeline.metadata import observation_function


narn_links_src = dagster.SourceAsset(
    key=dagster.AssetKey("narn_rail_network_links"),
    observe_fn=observation_function,
    description="Publicly available GIS data for railway network links",
    io_manager_key="custom_io_manager",
    metadata={
        "format": "zip",
        "filename": "narn_rail_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://opendata.arcgis.com/api/v3/datasets/d83e85154a304da995837889cc4012e3_0/downloads/data?format=shp&spatialRefId=4326&where=1%3D1"
        ),
    },
)
