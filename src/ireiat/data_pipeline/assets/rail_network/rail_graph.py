import dagster
import geopandas
import pandas as pd

import ireiat.data_pipeline.assets.rail_network.narn_filter
from ireiat.config import INTERMEDIATE_DIRECTORY_ARGS
from ireiat.data_pipeline.metadata import publish_metadata
from ireiat.util.graph import get_coordinates_from_geoframe, generate_zero_based_node_maps


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", "use_geopandas": True, **INTERMEDIATE_DIRECTORY_ARGS},
)
def filtered_and_processed_rail_network_links(
    context: dagster.AssetExecutionContext, narn_rail_network_links: geopandas.GeoDataFrame
) -> geopandas.GeoDataFrame:
    """Preprocess the rail links data by filtering records that are known to be defunct (e.g. NET=A, Abandoned)
    and consolidating the railroad owner(s) and railroad trackage rights into a single field"""
    processed_links = ireiat.data_pipeline.assets.rail_network.narn_filter.narn_links_preprocessing(
        narn_rail_network_links
    )
    context.log.info(
        f"Rail links data loaded and preprocessed with {processed_links.shape[0]} rail links"
    )
    publish_metadata(context, processed_links)
    return processed_links


@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def undirected_rail_edges(
    context: dagster.AssetExecutionContext,
    filtered_and_processed_rail_network_links: geopandas.GeoDataFrame,
) -> pd.DataFrame:
    """For each undirected edge in the rail dataset, create a row in the table with origin_lat, origin_long,
    destination_lat, and destination_long, along with several other edge fields of interest"""

    link_coords = get_coordinates_from_geoframe(filtered_and_processed_rail_network_links)
    fields_to_retain = ["link_id", "from_node", "to_node", "railroads", "miles"]
    link_coords = pd.concat(
        [filtered_and_processed_rail_network_links[fields_to_retain], link_coords], axis=1
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


# TODO (NP) - figure out how to attach intermodal terminals and the interplay with the impedances
# @dagster.asset(
#     io_manager_key="custom_io_manager",
#     metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
# )
# def rail_network_terminals(
#         context: dagster.AssetExecutionContext, intermodal_terminals: pd.DataFrame
# ) -> pd.DataFrame:
#     """Preprocess the intermodal terminals data"""
#     processed_terminals = data_handler.intermodal_terminals_preprocessing(intermodal_terminals)
#     context.log.info(
#         f"Intermodal terminals data loaded and preprocessed with {processed_terminals.shape[0]} terminals"
#     )
#     publish_metadata(context, processed_terminals)
#     return processed_terminals
