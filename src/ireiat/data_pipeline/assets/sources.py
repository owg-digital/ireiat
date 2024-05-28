import dagster


faf5_regions = dagster.SourceAsset(
    key=dagster.AssetKey("faf5_regions"),
    description="Publicly available GIS data for FAF5 regions",
    io_manager_key="custom_io_manager",
    metadata={
        "format": "zip",
        "filename": "faf5_regions.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://opendata.arcgis.com/api/v3/datasets/e3bcc5d26e5e42709e2bacd6fc37ab43_0/downloads/data?format=shp&spatialRefId=3857&where=1%3D1"
        ),
    },
)

us_county_shp_files = dagster.SourceAsset(
    key=dagster.AssetKey("us_county_shp_files"),
    description="Publicly available GIS data for US counties",
    io_manager_key="custom_io_manager",
    metadata={
        "format": "zip",
        "filename": "us_county_shp_files.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"
        ),
    },
)
