import dagster
import pandas as pd

from ireiat.data_pipeline.assets.demand.sources import US_CENSUS_COUNTY_POPULATION_DESCRIPTION
from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(io_manager_key="mem_io_manager", description=US_CENSUS_COUNTY_POPULATION_DESCRIPTION)
def us_census_county_population(
    context: dagster.AssetExecutionContext, us_census_county_population_src: pd.DataFrame
) -> pd.DataFrame:
    f""" {US_CENSUS_COUNTY_POPULATION_DESCRIPTION}"""
    publish_metadata(context, us_census_county_population_src)
    return us_census_county_population_src
