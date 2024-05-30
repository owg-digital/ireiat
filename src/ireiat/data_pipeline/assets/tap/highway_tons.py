from typing import Dict, Tuple

import dagster
import pandas as pd

from ireiat.config import EXCLUDED_FIPS_CODES_MAP
from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(io_manager_key="custom_io_manager", metadata={"format": "parquet"})
def in_network_highway_tons(
    context: dagster.AssetExecutionContext, county_to_county_highway_tons: pd.DataFrame
) -> pd.DataFrame:
    """Creates a file representing a subset of OD tons to consider in the TAP"""
    # eliminate states and territories we're not interested in
    relevant_county_ods = county_to_county_highway_tons.loc[
        ~(
            county_to_county_highway_tons["state_orig"].isin(EXCLUDED_FIPS_CODES_MAP.values())
            | county_to_county_highway_tons["state_dest"].isin(EXCLUDED_FIPS_CODES_MAP.values())
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
        f"County ODs: {len(county_to_county_highway_tons):,}, Relevant ODs: {len(relevant_county_ods):,}, Non-self ODs {len(non_self_county_ods):,}"
    )
    context.log.info(
        f"County Tons: {county_to_county_highway_tons['tons'].sum():,.1f}, Relevant tons: {relevant_county_ods['tons'].sum():,.1f}, Non-self tons {non_self_county_ods['tons'].sum():,.1f}"
    )

    # start with some limit of ODs
    OD_QUANTILE_THRESHOLD = 0.9999

    tons_threshold = non_self_county_ods["tons"].quantile(OD_QUANTILE_THRESHOLD)
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
    metadata={"format": "parquet", "write_kwargs": dagster.MetadataValue.json({"index": False})},
)
def tap_highway_tons(
    context: dagster.AssetExecutionContext,
    in_network_highway_tons: pd.DataFrame,
    county_fips_to_highway_network_node_idx: Dict[Tuple[str, str], int],
) -> pd.DataFrame:
    """Tons attached to the highway network nodes (from, to, tons)"""
    od_tuples = []
    for row in in_network_highway_tons.itertuples():
        orig = (row.state_orig, row.county_orig)
        dest = (row.state_dest, row.county_dest)
        od_tuples.append(
            (
                county_fips_to_highway_network_node_idx[orig],
                county_fips_to_highway_network_node_idx[dest],
                row.tons,
            )
        )

    trips = pd.DataFrame(od_tuples, columns=["from", "to", "tons"]).sort_values(["from", "to"])
    publish_metadata(context, trips)
    return trips
