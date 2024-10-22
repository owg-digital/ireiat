import dagster
import igraph

from ireiat.util.graph import generate_ball_tree


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def marine_ball_tree(
    strongly_connected_marine_graph: igraph.Graph,
):
    """BallTree for marine nodes from the marine graph"""
    return generate_ball_tree(strongly_connected_marine_graph)
