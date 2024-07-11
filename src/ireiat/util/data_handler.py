import geopandas
import pandas as pd


def narn_links_preprocessing(links_df: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    """
    Preprocess the NARN links dataframes.

    This function performs the following preprocessing steps:
    1. Ensure that the CRS for lat/longs are equal to EPSG:4326.
    2. Drop rows with missing values in 'FRAARCID', 'FRFRANODE', and 'TOFRANODE'.
    3. Convert 'FRAARCID', 'FRFRANODE', and 'TOFRANODE' columns to integers.
    4. Replace railroad owner and trackage rights codes according to a given mapping.
    5. Remove links that are abandoned or removed.
    6. Remove links that have AMTK as the sole owner with no other owner and no trackage rights.
    7. Create 'OWNERS' and 'TRKRIGHTS' columns from non-null values of respective columns.
    8. Add CSXT and NS to 'OWNERS' if PAS is one of the owners, since PAS is jointly owned by CSXT and NS.
    9. Create 'RAILROADS' column containing unique railroads from 'OWNERS' and 'TRKRIGHTS' (excluding AMTK).
    10. Rename specific columns for clarity.

    Parameters:
    links_df (geopandas.GeoDataFrame): DataFrame containing link data.

    Returns:
    geopandas.GeoDataFrame: Preprocessed links dataframe with renamed columns.
    """
    # Ensure that the CRS for lat/longs are equal to EPSG:4326
    links_df = links_df.to_crs("EPSG:4326")
    
    # Drop rows with missing values in 'FRAARCID', 'FRFRANODE', and 'TOFRANODE'
    links_df = links_df.dropna(subset=["FRAARCID", "FRFRANODE", "TOFRANODE"])

    # Convert 'FRAARCID', 'FRFRANODE', and 'TOFRANODE' to integers
    links_df["FRAARCID"] = links_df["FRAARCID"].astype(int)
    links_df["FRFRANODE"] = links_df["FRFRANODE"].astype(int)
    links_df["TOFRANODE"] = links_df["TOFRANODE"].astype(int)

    # Preprocess: Replace codes in RROWNER and TRKRGHTS columns
    rrowner_cols = [col for col in links_df.columns if "RROWNER" in col]
    trkrghts_cols = [col for col in links_df.columns if "TRKRGHTS" in col]
    columns_to_replace = rrowner_cols + trkrghts_cols

    rr_mapping_dict = {"CPRS": "CPKC", "KCS": "CPKC", "KCSM": "CPKC"}

    for col in columns_to_replace:
        links_df[col] = links_df[col].replace(rr_mapping_dict)

    # Remove links if the line is abandoned or removed
    links_df = links_df[~links_df["NET"].isin(["A", "R"])]

    # Remove links that have AMTK as the owner with no other owner and all TRKRGHTS columns are null
    amtk_filter = (
        (links_df["RROWNER1"] == "AMTK")
        & links_df["RROWNER2"].isna()
        & links_df["RROWNER3"].isna()
    )
    for col in trkrghts_cols:
        amtk_filter &= links_df[col].isna()

    links_df = links_df[~amtk_filter]

    # Create OWNERS column
    links_df["OWNERS"] = links_df[rrowner_cols].apply(
        lambda x: set(filter(pd.notna, x)), axis=1
    )

    # Add CSXT and NS to OWNERS if PAS is one of the owners (PAS is jointly owned by CSXT and NS)
    def add_csxt_ns(owners: set[str]) -> set[str]:
        owners_set = set(owners)
        if "PAS" in owners_set:
            owners_set.update(["CSXT", "NS"])
        return set(owners_set)

    links_df["OWNERS"] = links_df["OWNERS"].apply(add_csxt_ns)

    # Create TRKRIGHTS column
    links_df["TRKRIGHTS"] = links_df[trkrghts_cols].apply(
        lambda x: set(filter(pd.notna, x)), axis=1
    )

    # Function to exclude 'AMTK' from a set
    def exclude_amtk(railroads: set[str]) -> set[str]:
        return set(r for r in railroads if r != "AMTK")

    # Create RAILROADS column
    links_df["RAILROADS"] = links_df.apply(
        lambda x: exclude_amtk(x["OWNERS"]).union(exclude_amtk(x["TRKRIGHTS"])), axis=1
    )

    # Rename columns
    links_df_column_mapping = {
        "FRAARCID": "link_id",
        "FRFRANODE": "from_node",
        "TOFRANODE": "to_node",
        "STCNTYFIPS": "state_county_fips",
        "STATEAB": "state",
        "COUNTRY": "country",
        "YARDNAME": "yard_name",
        "PASSNGR": "passenger",
        "NET": "net",
        "MILES": "distance",
        "geometry": "geometry",
        "OWNERS": "owners",
        "TRKRIGHTS": "track_rights",
        "RAILROADS": "railroads",
    }

    # Apply the renaming
    links_df = links_df.rename(columns=links_df_column_mapping)

    # Return the dataframe with renamed columns
    return links_df[list(links_df_column_mapping.values())]


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
    terminals_df.loc[:, "FRA_RRS_AT_NODE"] = terminals_df["FRA_RRS_AT_NODE"].apply(
        clean_railroads
    )
    terminals_df.loc[:, "CLOSEST_FRA_RRS"] = terminals_df["CLOSEST_FRA_RRS"].apply(
        clean_railroads
    )

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
