import geopandas
import pandas as pd


def narn_links_preprocessing(links_df: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    """
    Preprocess the NARN links dataframes.

    This function performs the following preprocessing steps:
    1. Drop rows with missing values in 'FRAARCID', 'FRFRANODE', and 'TOFRANODE'.
    2. Convert 'FRAARCID', 'FRFRANODE', and 'TOFRANODE' columns to integers.
    3. Replace railroad owner and trackage rights codes according to a given mapping.
    4. Remove links that are abandoned or removed.
    5. Remove links that have AMTK as the sole owner with no other owner and no trackage rights.
    6. Create 'OWNERS' and 'TRKRIGHTS' columns from non-null values of respective columns.
    7. Add CSXT and NS to 'OWNERS' if PAS is one of the owners, since PAS is jointly owned by CSXT and NS.
    8. Create 'RAILROADS' column containing unique railroads from 'OWNERS' and 'TRKRIGHTS' (excluding AMTK).
    9. Rename specific columns for clarity.

    Parameters:
    links_df (geopandas.GeoDataFrame): DataFrame containing link data.

    Returns:
    geopandas.GeoDataFrame: Preprocessed links dataframe with renamed columns.
    """
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
        (links_df["RROWNER1"] == "AMTK") & links_df["RROWNER2"].isna() & links_df["RROWNER3"].isna()
    )
    for col in trkrghts_cols:
        amtk_filter &= links_df[col].isna()

    links_df = links_df[~amtk_filter]

    # Create OWNERS column
    links_df["OWNERS"] = links_df[rrowner_cols].apply(lambda x: set(filter(pd.notna, x)), axis=1)

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
        "MILES": "miles",
        "geometry": "geometry",
        "OWNERS": "owners",
        "TRKRIGHTS": "track_rights",
        "RAILROADS": "railroads",
    }

    # Apply the renaming
    links_df = links_df.rename(columns=links_df_column_mapping)

    # Return the dataframe with renamed columns
    return links_df[list(links_df_column_mapping.values())]
