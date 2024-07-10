import dagster
import geopandas 
import pandas as pd

import ireiat.data_pipeline.assets.rail_network.data_handler as data_handler

# Constants
INTERMEDIATE_DIRECTORY_ARGS = {"intermediate_dir": "/path/to/intermediate/dir"}

@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def rail_network_links(context: dagster.AssetExecutionContext, narn_rail_network_links: geopandas.GeoDataFrame) -> pd.DataFrame:
    """Preprocess the rail links data"""
    processed_links = data_handler.narn_links_preprocessing(narn_rail_network_links)
    context.log.info(f"Rail links data loaded and preprocessed with {processed_links.shape[0]} rail links")
    return processed_links

@dagster.asset(
    io_manager_key="custom_io_manager",
    metadata={"format": "parquet", **INTERMEDIATE_DIRECTORY_ARGS},
)
def rail_network_terminals(context: dagster.AssetExecutionContext, intermodal_terminals: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the intermodal terminals data"""
    processed_terminals = data_handler.intermodal_terminals_preprocessing(intermodal_terminals)
    context.log.info(f"Intermodal terminals data loaded and preprocessed with {processed_terminals.shape[0]} terminals")
    return processed_terminals
