import dagster

from ireiat.data_pipeline.io_manager import asset_spec_factory

FAF5_REGIONS_DESCRIPTION = "Publicly available GIS data for FAF5 regions"
faf5_regions_spec = dagster.AssetSpec(
    key=dagster.AssetKey("faf5_regions_spec"),
    description=FAF5_REGIONS_DESCRIPTION,
    metadata={
        "format": "zip",
        "filename": "faf5_regions.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://hub.arcgis.com/api/download/v1/items/539fdab88ee74e38b28959494965ace2/shapefile?redirect=true&layers=0"
        ),
    },
)

faf5_regions_src = asset_spec_factory(faf5_regions_spec)

US_COUNTY_SHP_FILES_DESCRIPTION = "Publicly available GIS data for US counties"
us_county_shp_files_spec = dagster.AssetSpec(
    key=dagster.AssetKey("us_county_shp_files_spec"),
    description=US_COUNTY_SHP_FILES_DESCRIPTION,
    metadata={
        "format": "zip",
        "filename": "us_county_shp_files.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"
        ),
    },
)
us_county_shp_files_src = asset_spec_factory(us_county_shp_files_spec)

US_CENSUS_COUNTY_POPULATION_DESCRIPTION = (
    "Publicly available census population data for US counties"
)
us_census_county_population_spec = dagster.AssetSpec(
    key=dagster.AssetKey("us_census_county_population_spec"),
    description=US_CENSUS_COUNTY_POPULATION_DESCRIPTION,
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
us_census_county_population_src = asset_spec_factory(us_census_county_population_spec)

FAF_DEMAND_DESCRIPTION = "FAF5 Framework tonnage, ton miles, and values"
faf5_demand_spec = dagster.AssetSpec(
    key=dagster.AssetKey("faf5_demand_spec"),
    description=FAF_DEMAND_DESCRIPTION,
    metadata={
        "format": "zip",
        "filename": "faf5_demand.zip",
        "temp_unzip": True,
        "source_path": "raw/",
        "read_kwargs": dagster.MetadataValue.json(
            {"dtype": {"dms_orig": "str", "dms_dest": "str", "sctg2": "str"}}
        ),
        "dashboard_url": dagster.MetadataValue.url(
            "https://faf.ornl.gov/faf5/data/download_files/FAF5.5.1.zip"
        ),
    },
)
faf5_demand_src = asset_spec_factory(faf5_demand_spec)
