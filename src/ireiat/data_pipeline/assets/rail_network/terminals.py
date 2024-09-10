import re
import dagster
import pandas as pd
import igraph as ig
from math import isclose
from typing import Optional, Dict, Tuple, Set

from ireiat.util.rail_network_constants import VertexType, EdgeType


def _get_node_index(
    row: pd.Series,
    complete_rail_node_to_idx: Dict[Tuple[float, float], int],
    tolerance: float = 1e-6,
) -> Optional[int]:
    """
    Returns the index of a rail node based on the latitude and longitude of a given row, allowing small tolerance for matching.

    Args:
        row (pd.Series): A row containing 'latitude' and 'longitude' columns.
        complete_rail_node_to_idx (Dict[Tuple[float, float], int]): A dictionary mapping (latitude, longitude)
            pairs to rail node indices.
        tolerance (float): The allowed tolerance for lat/lon differences.

    Returns:
        Optional[int]: The index of the rail node, or None if not found.
    """
    rounded_lat_lon = (round(row["latitude"], 6), round(row["longitude"], 6))

    # Iterate over dictionary keys and check for a match within the tolerance
    for (lat, lon), idx in complete_rail_node_to_idx.items():
        if isclose(lat, rounded_lat_lon[0], abs_tol=tolerance) and isclose(
            lon, rounded_lat_lon[1], abs_tol=tolerance
        ):

            return idx

    return None


def intermodal_terminals_preprocessing(
    terminals_df: pd.DataFrame, complete_rail_node_to_idx: Dict[Tuple[float, float], int]
) -> pd.DataFrame:
    """
    Preprocesses the intermodal terminals DataFrame.

    This function cleans and prepares the terminals data by:
    1. Dropping rows marked for discard.
    2. Converting railroad information into a set of railroads.
    3. Renaming relevant columns for consistency.
    4. Mapping terminal coordinates to corresponding rail node indices.

    Args:
        terminals_df (pd.DataFrame): DataFrame containing terminal data.
        complete_rail_node_to_idx (Dict[Tuple[float, float], int]): A dictionary mapping (latitude, longitude)
            pairs to corresponding rail node indices.

    Returns:
        pd.DataFrame: The preprocessed terminals DataFrame with cleaned data and node indices.
    """
    # Drop rows where DISCARD == 'Y'
    terminals_df = terminals_df[terminals_df["DISCARD"] != "Y"].copy()

    # Function to clean railroads by converting them into a set and stripping whitespaces
    def clean_railroads(railroads: str) -> set[str]:
        if pd.isna(railroads):
            return set()
        # Handle both comma-separated and set-like strings
        railroads = railroads.strip("{}")
        return set(r.strip().strip("'") for r in railroads.split(","))

    # Apply the cleaning function to the 'FRA_RRS_TO_MATCH' column
    terminals_df["FRA_RRS_TO_MATCH"] = terminals_df["FRA_RRS_TO_MATCH"].apply(clean_railroads)

    # Rename columns for consistency and clarity
    terminals_df_column_mapping = {
        "TERMINAL": "terminal_name",
        "FRA_RRS_TO_MATCH": "railroads",
        "LAT_AT_NODE": "latitude",
        "LON_AT_NODE": "longitude",
    }

    terminals_df = terminals_df.rename(columns=terminals_df_column_mapping)

    # Keep only the renamed columns
    terminals_df = terminals_df[list(terminals_df_column_mapping.values())]

    # Map the terminal's latitude and longitude to a rail node index using the rounded dictionary
    terminals_df["node_idx"] = terminals_df.apply(
        _get_node_index, axis=1, complete_rail_node_to_idx=complete_rail_node_to_idx
    )

    return terminals_df


def _extract_owner(original_idx: str) -> Optional[str]:
    """
    Extracts the alphabetic prefix (owner) from the original index.

    Args:
        original_idx (str): The original index containing the owner prefix.

    Returns:
        Optional[str]: The extracted owner (alphabetic prefix) or None if not found.
    """
    # Use regular expression to match the alphabetic prefix
    match = re.match(r"([A-Za-z]+)", original_idx)

    if match:
        return match.group(1)

    return None


def update_impedance_graph_with_terminals(
    context: dagster.AssetExecutionContext,
    terminals_df: pd.DataFrame,
    impedance_rail_graph: ig.Graph,
) -> ig.Graph:
    """
    Updates the impedance rail graph by adding terminal nodes and edges.
    Only adds terminals that successfully connect to the rest of the graph.

    Args:
        context (dagster.AssetExecutionContext): Dagster execution context for logging.
        terminals_df (pd.DataFrame): DataFrame containing preprocessed terminal data.
        impedance_rail_graph (igraph.Graph): The impedance rail graph to be updated.

    Returns:
        ig.Graph: The updated impedance rail graph with terminal nodes and edges.
    """
    for row in terminals_df.itertuples(index=False):
        node_idx = row.node_idx

        # Skip rows with missing node_idx and log a warning
        if pd.isnull(node_idx):
            context.log.warning(f"Terminal {row.terminal_name} skipped: Missing node_idx.")
            continue

        terminal_operators: Set[str] = row.railroads

        # Find vertices with the matching node index in the graph
        vertices = impedance_rail_graph.vs.select(original_node_idx=node_idx)

        # Track which terminal operators have been connected to a vertex
        connected_railroads = set()
        other_vertex = None  # Track if we find a vertex with 'Other' as the owner

        # Check if we can connect the terminal to any existing vertices
        for v in vertices:
            vertex_owner = _extract_owner(v["original_idx"])

            if vertex_owner in terminal_operators:
                connected_railroads.add(vertex_owner)
            elif vertex_owner == "Other" and other_vertex is None:
                other_vertex = v

        unconnected_railroads = set(terminal_operators) - connected_railroads

        # If no valid connections (either to terminal operators or "Other" vertices), log a warning and skip this terminal
        if not connected_railroads and not other_vertex:
            context.log.warning(f"Terminal {row.terminal_name} skipped: No valid connections.")
            continue

        # Create two new vertices for the terminal (only if we know it will connect to the graph)
        terminal_vertex_1 = impedance_rail_graph.add_vertex()
        terminal_vertex_2 = impedance_rail_graph.add_vertex()

        # Assign attributes to the new terminal vertices
        terminal_vertex_1["terminal_name"] = row.terminal_name
        terminal_vertex_1["vertex_type"] = VertexType.IM_TERMINAL.value
        terminal_vertex_1["owners"] = terminal_operators
        terminal_vertex_1["latitude"] = round(row.latitude, 6)
        terminal_vertex_1["longitude"] = round(row.longitude, 6)

        terminal_vertex_2["terminal_name"] = row.terminal_name
        terminal_vertex_2["vertex_type"] = VertexType.IM_DUMMY_NODE.value
        terminal_vertex_2["owners"] = terminal_operators
        terminal_vertex_2["latitude"] = round(row.latitude, 6)
        terminal_vertex_2["longitude"] = round(row.longitude, 6)

        # Add intermodal capacity edges between the two new vertices in both directions
        im_capacity_edge_1 = impedance_rail_graph.add_edge(terminal_vertex_1, terminal_vertex_2)
        im_capacity_edge_2 = impedance_rail_graph.add_edge(terminal_vertex_2, terminal_vertex_1)

        # Assign attributes to the intermodal capacity edges
        for im_capacity_edge in [im_capacity_edge_1, im_capacity_edge_2]:
            im_capacity_edge["edge_type"] = EdgeType.IM_CAPACITY.value
            im_capacity_edge["owners"] = ",".join(terminal_operators)

        # Now connect the dummy terminal vertex to the existing rail vertices
        for v in vertices:
            vertex_owner = _extract_owner(v["original_idx"])

            # Create real edges if the vertex owner is one of the terminal operators
            if vertex_owner in terminal_operators:
                connected_railroads.add(vertex_owner)

                # Create edges from terminal_vertex_2 to the existing vertex and vice versa
                for direction in [
                    (terminal_vertex_2, v, EdgeType.RAIL_TO_QUANT.value),
                    (v, terminal_vertex_2, EdgeType.QUANT_TO_RAIL.value),
                ]:
                    new_edge = impedance_rail_graph.add_edge(direction[0], direction[1])
                    new_edge["edge_type"] = direction[2]
                    new_edge["owners"] = vertex_owner

            # Handle 'Other' vertices (if no matching railroad is found)
            elif vertex_owner == "Other" and other_vertex is None:
                other_vertex = v  # Track the first 'Other' vertex found

        # Connect any unconnected terminal operators to the 'Other' vertex, if it exists
        if unconnected_railroads and other_vertex:
            # Create edges from terminal_vertex_2 to 'Other' vertex and vice versa
            for direction in [
                (terminal_vertex_2, other_vertex, EdgeType.RAIL_TO_QUANT.value),
                (other_vertex, terminal_vertex_2, EdgeType.QUANT_TO_RAIL.value),
            ]:
                new_edge = impedance_rail_graph.add_edge(direction[0], direction[1])
                new_edge["edge_type"] = direction[2]
                new_edge["owners"] = "Other"

    return impedance_rail_graph
