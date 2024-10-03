from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Optional

from ireiat.config.constants import INTERMEDIATE_PATH, CACHE_PATH, RAW_PATH

intermediate_path = CACHE_PATH / INTERMEDIATE_PATH
raw_path = CACHE_PATH / RAW_PATH

default_highway_traffic_output_path = CACHE_PATH / "highway_traffic.parquet"
default_marine_traffic_output_path = CACHE_PATH / "marine_traffic.parquet"
default_rail_traffic_output_path = CACHE_PATH / "rail_traffic.parquet"


@dataclass
class RunConfig:
    """Simple run configuration allowing for default overrides in child classes or passed paths"""

    default_network_file_path: Path = None
    default_od_file_path: Path = None
    default_output_file_path: Path = None

    passed_network_file_path: Optional[Path] = None
    passed_od_file_path: Optional[Path] = None
    passed_output_file_path: Optional[Path] = None

    @property
    def network_file_path(self):
        return self.passed_network_file_path or self.default_network_file_path

    @property
    def od_file_path(self):
        return self.passed_od_file_path or self.default_od_file_path

    @property
    def output_file_path(self):
        return self.passed_output_file_path or self.default_output_file_path


marine_config = partial(
    RunConfig,
    default_network_file_path=intermediate_path / "tap_marine_network_dataframe.parquet",
    default_od_file_path=intermediate_path / "tap_marine_tons.parquet",
    default_output_file_path=default_marine_traffic_output_path,
)

highway_config = partial(
    RunConfig,
    default_network_file_path=intermediate_path / "tap_highway_network_dataframe.parquet",
    default_od_file_path=intermediate_path / "tap_highway_tons.parquet",
    default_output_file_path=default_highway_traffic_output_path,
)

rail_config = partial(
    RunConfig,
    default_network_file_path=intermediate_path / "tap_rail_network_dataframe.parquet",
    default_od_file_path=intermediate_path / "tap_rail_tons.parquet",
    default_output_file_path=default_rail_traffic_output_path,
)

run_config_map = {"marine": marine_config, "highway": highway_config, "rail": rail_config}


@dataclass
class PostprocessConfig:
    """Simple postprocess configuration allowing for default overrides in child classes or passed paths"""

    default_traffic_path: Path = None
    default_network_graph_path: Path = None
    default_shp_file_path: Path = None

    passed_traffic_path: Optional[Path] = None
    passed_network_graph_path: Optional[Path] = None
    passed_shp_file_path: Optional[Path] = None

    @property
    def traffic_file_path(self):
        return self.passed_traffic_path or self.default_traffic_path

    @property
    def network_graph_path(self):
        return self.passed_network_graph_path or self.default_network_graph_path

    @property
    def shp_file_path(self):
        return self.passed_shp_file_path or self.default_shp_file_path


highway_postprocess_config = partial(
    PostprocessConfig,
    default_traffic_path=default_highway_traffic_output_path,
    default_network_graph_path=intermediate_path / "strongly_connected_highway_graph",
    default_shp_file_path=raw_path / "faf5_highway_links.zip",
)

marine_postprocess_config = partial(
    PostprocessConfig,
    default_traffic_path=default_marine_traffic_output_path,
    default_network_graph_path=intermediate_path / "strongly_connected_marine_graph",
    default_shp_file_path=raw_path / "marine_links.zip",
)

rail_postprocess_config = partial(
    PostprocessConfig,
    default_traffic_path=default_rail_traffic_output_path,
    default_network_graph_path=intermediate_path / "rail_graph_with_county_connections",
    default_shp_file_path=raw_path / "narn_rail_links.zip",
)

postprocess_config_map = {
    "marine": marine_postprocess_config,
    "highway": highway_postprocess_config,
    "rail": rail_postprocess_config,
}
