import dagster
import geopandas
import pandas as pd

from ireiat.config import INTERMEDIATE_DIRECTORY_ARGS
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.graph import get_coordinates_from_geoframe, generate_zero_based_node_maps


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def undirected_rail_edges(
    context: dagster.AssetExecutionContext, narn_rail_network_links: geopandas.GeoDataFrame
) -> pd.DataFrame:
    """For each undirected edge in the rail dataset, create a row in the table with origin_lat, origin_long,
    destination_lat, and destination_long, along with several other edge fields of interest"""
    narn_rail_network_links.columns = [c.lower() for c in narn_rail_network_links.columns]
    link_coords = get_coordinates_from_geoframe(narn_rail_network_links)
    link_coords = pd.concat(
        [narn_rail_network_links[["miles", "frfranode", "tofranode"]], link_coords], axis=1
    )  # join in the direction
    publish_metadata(context, link_coords)
    return link_coords


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_rail_node_to_idx(undirected_rail_edges: pd.DataFrame):
    """Generate unique nodes->indices based on the entire rail network"""
    return generate_zero_based_node_maps(undirected_rail_edges)


@dagster.asset(io_manager_key="default_io_manager_intermediate_path")
def complete_rail_idx_to_node(complete_rail_node_to_idx):
    """Generates unique indices->nodes based on the entire rail network"""
    return {v: k for k, v in complete_rail_node_to_idx.items()}
