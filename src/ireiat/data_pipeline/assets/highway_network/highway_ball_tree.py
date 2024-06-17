from typing import Dict, Tuple

import dagster
import igraph as ig
import numpy as np
from sklearn.neighbors import BallTree


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def highway_ball_tree(
    strongly_connected_highway_graph: ig.Graph,
    complete_highway_idx_to_node: Dict[int, Tuple[float, float]],
):
    """BallTree for highway nodes from the strongly connected subgraph"""
    new_lat_longs = [
        complete_highway_idx_to_node[new_node["original_node_idx"]]
        for new_node in strongly_connected_highway_graph.vs
    ]
    highway_node_lat_long_radians = np.deg2rad(np.array(new_lat_longs))
    return BallTree(highway_node_lat_long_radians, metric="haversine")
