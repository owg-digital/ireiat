import pandas as pd
from collections import defaultdict
from typing import Dict, Tuple


def faf5_process_mode(
    unknown_modes_pdf: pd.DataFrame,
    known_modes_pdf: pd.DataFrame,
    mode: int,
    mode_tons_column: str,
    mode_pct_column: str,
    FAF_TONS_TARGET_FIELD: str,
) -> pd.DataFrame:
    """
    Process unknown mode rows by calculating tons for a specific mode, appending to the known mode DataFrame,
    and re-aggregating the data at the origin, destination, and tons level.

    Args:
        unknown_modes_pdf (pd.DataFrame): DataFrame containing unknown mode data, including origin, destination, and tons.
        known_modes_pdf (pd.DataFrame): DataFrame containing known mode data.
        mode (int): Mode of transport to process (e.g., 'truck', 'rail', 'water').
        mode_tons_column (str): Column name where the calculated tons for the mode will be stored.
        mode_pct_column (str): Column name containing the percentage of tons allocated to this mode.
        FAF_TONS_TARGET_FIELD (str): Name of the target column representing the number of tons in the known mode DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with the known and unknown mode data combined, re-aggregated at the origin, destination,
        and tons level for the specified mode.
    """
    # Calculate tons for the mode
    unknown_modes_pdf[mode_tons_column] = (
        unknown_modes_pdf[FAF_TONS_TARGET_FIELD] * unknown_modes_pdf[mode_pct_column]
    )

    # Append to the respective known mode DataFrame
    mode_pdf = known_modes_pdf[known_modes_pdf["dms_mode"] == mode].copy()
    mode_pdf = pd.concat(
        [
            mode_pdf,
            unknown_modes_pdf[["dms_orig", "dms_dest", mode_tons_column]].rename(
                columns={mode_tons_column: FAF_TONS_TARGET_FIELD}
            ),
        ]
    )

    # Re-aggregate at origin, destination, and tons level
    mode_pdf = mode_pdf.groupby(["dms_orig", "dms_dest"], as_index=False)[
        FAF_TONS_TARGET_FIELD
    ].sum()

    return mode_pdf


def faf5_compute_county_tons_for_mode(
    faf_demand_pdf: pd.DataFrame,
    faf_id_to_county_id_allocation_map: Dict[str, Dict[Tuple[str, str], float]],
    FAF_TONS_TARGET_FIELD: str,
    SUM_TONS_TOLERANCE: float,
    mode_name: str,
) -> pd.DataFrame:
    """
    Compute county-to-county tons for a specific mode, distributing based on FAF zone to county allocation percentages.

    Args:
        faf_demand_pdf (pd.DataFrame): DataFrame containing demand data for a specific mode (truck, rail, water).
        faf_id_to_county_id_allocation_map (dict): Mapping from FAF zones to county allocation percentages.
        FAF_TONS_TARGET_FIELD (str): The column in the DataFrame that holds the tons of demand.
        SUM_TONS_TOLERANCE (float): Tolerance for checking the sum of tons.
        mode_name (str): Name of the mode (e.g., "Truck", "Rail", "Water") for error reporting.

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
    ), f"Tons mismatch for {mode_name} mode."

    # Create a dataframe from the county_od dictionary
    county_od_pdf = pd.DataFrame(
        [(*k, v) for k, v in county_od.items()],
        columns=["state_orig", "county_orig", "state_dest", "county_dest", "tons"],
    )

    # Filter out rows with zero tons and sort
    non_zero_county_od_pdf = county_od_pdf.loc[county_od_pdf["tons"] > 0].sort_values("tons")

    return non_zero_county_od_pdf
