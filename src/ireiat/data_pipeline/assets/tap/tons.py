from typing import Dict, Tuple, Optional

import dagster
import pandas as pd

from ireiat.config import EXCLUDED_FIPS_CODES_MAP, INTERMEDIATE_DIRECTORY_ARGS
from ireiat.data_pipeline.metadata import publish_metadata


def _generate_tons_dataframe(
    context: dagster.AssetExecutionContext,
    in_network_tons: pd.DataFrame,
    county_fips_to_network_node_idx: Dict[Tuple[str, str], int],
) -> pd.DataFrame:
    """Helper function to associate (STATE, COUNTY) tons to a target network node ID mapping"""
    od_tuples = []
    for row in in_network_tons.itertuples():
        orig = (row.state_orig, row.county_orig)
        dest = (row.state_dest, row.county_dest)
        od_tuples.append(
            (
                county_fips_to_network_node_idx[orig],
                county_fips_to_network_node_idx[dest],
                row.tons,
            )
        )

    trips = pd.DataFrame(od_tuples, columns=["from", "to", "tons"]).sort_values(["from", "to"])
    publish_metadata(context, trips)
    return trips


def _filter_tons_dataframe(
    context: dagster.AssetExecutionContext,
    tons_dataframe: pd.DataFrame,
    quantile_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """Filters a state_orig | county_orig | state_dest | county_dest to exclude counties outside
    the continential US and to exclude counties with self-circulating flows. If `quantile_threshold`
    is specified, the dataframe is further filtered to include only tons above that quantile."""
    # eliminate states and territories we're not interested in
    relevant_county_ods = tons_dataframe.loc[
        ~(
            tons_dataframe["state_orig"].isin(EXCLUDED_FIPS_CODES_MAP.values())
            | tons_dataframe["state_dest"].isin(EXCLUDED_FIPS_CODES_MAP.values())
        )
    ]

    # eliminate "self-circulating" flows (same county -> same county)
    non_self_county_ods = relevant_county_ods.loc[
        ~(
            (relevant_county_ods["state_orig"] == relevant_county_ods["state_dest"])
            & (relevant_county_ods["county_orig"] == relevant_county_ods["county_dest"])
        )
    ]
    context.log.info(
        f"County ODs: {len(tons_dataframe):,}, Relevant ODs: {len(relevant_county_ods):,}, Non-self ODs {len(non_self_county_ods):,}"
    )
    context.log.info(
        f"County Tons: {tons_dataframe['tons'].sum():,.1f}, Relevant tons: {relevant_county_ods['tons'].sum():,.1f}, Non-self tons {non_self_county_ods['tons'].sum():,.1f}"
    )

    # start with some limit of ODs
    if quantile_threshold is None:
        return non_self_county_ods.reset_index(drop=True)

    tons_threshold = non_self_county_ods["tons"].quantile(quantile_threshold)
    subset_county_od = non_self_county_ods.loc[non_self_county_ods["tons"] > tons_threshold]

    subset_county_od = subset_county_od.sort_values(
        ["state_orig", "county_orig", "state_dest", "county_dest"]
    )
    context.log.info(subset_county_od["tons"].describe())
    context.log.info(subset_county_od["tons"].sum() / non_self_county_ods["tons"].sum())
    reindexed_subset_county_od = subset_county_od.reset_index(drop=True)
    publish_metadata(context, reindexed_subset_county_od)
    return reindexed_subset_county_od


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def in_network_highway_tons(
    context: dagster.AssetExecutionContext, county_to_county_highway_tons: pd.DataFrame
) -> pd.DataFrame:
    """Creates a file representing a subset of highway OD tons to consider in the TAP"""
    # TODO (NP) - eliminate these filters such that we can solve at scale
    return _filter_tons_dataframe(context, county_to_county_highway_tons, quantile_threshold=0.999)


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def in_network_marine_tons(
    context: dagster.AssetExecutionContext, county_to_county_marine_tons: pd.DataFrame
) -> pd.DataFrame:
    """Creates a file representing a subset of marine OD tons to consider in the TAP"""
    # TODO (NP) - eliminate these filters such that we can solve at scale
    return _filter_tons_dataframe(context, county_to_county_marine_tons, quantile_threshold=0.5)


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_highway_tons(
    context: dagster.AssetExecutionContext,
    in_network_highway_tons: pd.DataFrame,
    county_fips_to_highway_network_node_idx: Dict[Tuple[str, str], int],
) -> pd.DataFrame:
    """Tons attached to the highway network nodes (from, to, tons)"""
    return _generate_tons_dataframe(
        context, in_network_highway_tons, county_fips_to_highway_network_node_idx
    )


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={
        "format": "parquet",
        "write_kwargs": dagster.MetadataValue.json(
            {"index": False},
        ),
        **INTERMEDIATE_DIRECTORY_ARGS,
    },
)
def tap_marine_tons(
    context: dagster.AssetExecutionContext,
    in_network_marine_tons: pd.DataFrame,
    county_fips_to_marine_network_node_idx: Dict[Tuple[str, str], int],
) -> pd.DataFrame:
    """Tons attached to the marine network nodes (from, to, tons)"""
    return _generate_tons_dataframe(
        context, in_network_marine_tons, county_fips_to_marine_network_node_idx
    )
