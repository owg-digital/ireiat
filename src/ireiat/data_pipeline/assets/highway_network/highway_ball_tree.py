import dagster
import pandas as pd

from ireiat.util.graph import generate_ball_tree


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def highway_ball_tree(
    highway_network_dataframe: pd.DataFrame,
):
    """BallTree for highway nodes from the highway network dataframe"""
    return generate_ball_tree(highway_network_dataframe)
