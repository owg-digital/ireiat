import pandas as pd


def intermodal_terminals_preprocessing(terminals_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the terminals DataFrame.

    Args:
        terminals_df (pd.DataFrame): DataFrame containing terminal data.

    Returns:
        pd.DataFrame: Preprocessed terminals dataframe
    """
    # Drop rows where DISCARD == 'Y'
    terminals_df = terminals_df[terminals_df["DISCARD"] != "Y"].copy()

    # Drop rows with missing values in 'FRANODEID'
    terminals_df = terminals_df.dropna(subset=["FRANODEID"])

    # Convert 'FRANODEID' and 'CLOSEST_FRANODEID' to integers
    terminals_df["FRANODEID"] = terminals_df["FRANODEID"].astype(int)
    terminals_df["CLOSEST_FRANODEID"] = terminals_df["CLOSEST_FRANODEID"].astype(int)

    # Split railroads_to_match into sets and strip whitespaces
    def clean_railroads(railroads: str) -> set[str]:
        if pd.isna(railroads):
            return set()
        # Handle both comma-separated and set-like strings
        railroads = railroads.strip("{}")
        return set(r.strip().strip("'") for r in railroads.split(","))

    terminals_df.loc[:, "FRA_RRS_TO_MATCH"] = terminals_df["FRA_RRS_TO_MATCH"].apply(
        clean_railroads
    )
    terminals_df.loc[:, "FRA_RRS_AT_NODE"] = terminals_df["FRA_RRS_AT_NODE"].apply(clean_railroads)
    terminals_df.loc[:, "CLOSEST_FRA_RRS"] = terminals_df["CLOSEST_FRA_RRS"].apply(clean_railroads)

    # Rename columns
    terminals_df_column_mapping = {
        "idx": "id",
        "TERMINAL": "terminal_name",
        "FRA_RRS_TO_MATCH": "railroads",
        "FRANODEID": "rail_node",
        "FRA_YARD_NAME": "yard_name",
    }

    terminals_df = terminals_df.rename(columns=terminals_df_column_mapping)

    return terminals_df[list(terminals_df_column_mapping.values())]
