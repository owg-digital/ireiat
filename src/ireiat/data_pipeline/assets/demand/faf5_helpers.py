import pandas as pd


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
