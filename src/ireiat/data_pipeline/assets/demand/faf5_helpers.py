import pandas as pd
from collections import defaultdict
from typing import Dict, Tuple


def faf5_compute_county_tons_for_mode(
    faf_demand_pdf: pd.DataFrame,
    faf_id_to_county_id_allocation_map: Dict[str, Dict[Tuple[str, str], float]],
    FAF_TONS_TARGET_FIELD: str,
    SUM_TONS_TOLERANCE: float,
) -> pd.DataFrame:
    """
    Compute county-to-county tons for a specific mode, distributing based on FAF zone to county allocation percentages.

    Args:
        faf_demand_pdf (pd.DataFrame): DataFrame containing demand data for a specific mode (truck, rail, water).
        faf_id_to_county_id_allocation_map (dict): Mapping from FAF zones to county allocation percentages.
        FAF_TONS_TARGET_FIELD (str): The column in the DataFrame that holds the tons of demand.
        SUM_TONS_TOLERANCE (float): Tolerance for checking the sum of tons.

    Returns:
        pd.DataFrame: DataFrame with non-zero tons, aggregated at the county-to-county level.
    """
    county_od: Dict[Tuple[str, str, str, str], float] = defaultdict(float)
    for row in faf_demand_pdf.itertuples():
        constituent_orig_counties_map = faf_id_to_county_id_allocation_map[row.dms_orig]
        constituent_dest_counties_map = faf_id_to_county_id_allocation_map[row.dms_dest]
        for (state_orig, county_orig), pct_in_county_orig in constituent_orig_counties_map.items():
            for (
                state_dest,
                county_dest,
            ), pct_in_county_dest in constituent_dest_counties_map.items():
                tons = getattr(row, FAF_TONS_TARGET_FIELD) * pct_in_county_orig * pct_in_county_dest
                county_od[(state_orig, county_orig, state_dest, county_dest)] += tons

    # Verify that the total tons sum within tolerance
    assert (
        abs(faf_demand_pdf[FAF_TONS_TARGET_FIELD].sum() - sum(county_od.values()))
        < SUM_TONS_TOLERANCE
    ), "Tons mismatch for mode."

    # Create a dataframe from the county_od dictionary
    county_od_pdf = pd.DataFrame(
        [(*k, v) for k, v in county_od.items()],
        columns=["state_orig", "county_orig", "state_dest", "county_dest", "tons"],
    )

    # Filter out rows with zero tons and sort
    non_zero_county_od_pdf = county_od_pdf.loc[county_od_pdf["tons"] > 0].sort_values("tons")

    return non_zero_county_od_pdf
