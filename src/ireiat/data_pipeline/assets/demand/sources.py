import dagster


FAF5_REGIONS_DESCRIPTION = "Publicly available GIS data for FAF5 regions"
faf5_regions_src = dagster.SourceAsset(
    key=dagster.AssetKey("faf5_regions_src"),
    description=FAF5_REGIONS_DESCRIPTION,
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

US_COUNTY_SHP_FILES_DESCRIPTION = "Publicly available GIS data for US counties"
us_county_shp_files_src = dagster.SourceAsset(
    key=dagster.AssetKey("us_county_shp_files_src"),
    description=US_COUNTY_SHP_FILES_DESCRIPTION,
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

US_CENSUS_COUNTY_POPULATION_DESCRIPTION = (
    "Publicly available census population data for US counties"
)
us_census_county_population_src = dagster.SourceAsset(
    key=dagster.AssetKey("us_census_county_population_src"),
    description=US_CENSUS_COUNTY_POPULATION_DESCRIPTION,
    io_manager_key="custom_io_manager",
    metadata={
        "format": "csv",
        "filename": "co-est2022-alldata.csv",
        "source_path": "raw/",
        "read_kwargs": dagster.MetadataValue.json({"dtype": {"STATE": "str", "COUNTY": "str"}}),
        "dashboard_url": dagster.MetadataValue.url(
            "https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/counties/totals/co-est2022-alldata.csv"
        ),
    },
)
