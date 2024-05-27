import dagster
import geopandas


@dagster.asset(io_manager_key="default_io_manager")
def faf_id_to_county_areas(faf5_regions: geopandas.GeoDataFrame):
    """Reads FAF5 GIS data, county GIS data, and computes a FAF -> County -> % area map"""
    faf_zone_to_county = dict()
    for row in faf5_regions.itertuples():
        faf_zone_to_county[row.faf_zone] = 0
