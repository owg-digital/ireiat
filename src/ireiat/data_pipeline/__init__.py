from collections.abc import Sequence

import dagster

from .assets import demand, highway_network, tap
from .io_manager import TabularDataLocalIOManager
from ..config import CACHE_PATH, INTERMEDIATE_PATH


def _filter_for_non_source_assets(assets: Sequence) -> Sequence:
    """Retuns a sequence of non dagster.SourceAsset objects"""
    return [a for a in assets if not isinstance(a, dagster.SourceAsset)]


# demand
demand_assets = dagster.load_assets_from_package_module(demand, group_name="demand")
demand_assets_job = dagster.define_asset_job(
    name="highway_demand_job", selection=_filter_for_non_source_assets(demand_assets)
)

# highway network
highway_network_assets = dagster.load_assets_from_package_module(
    highway_network, group_name="highway_network"
)
highway_network_assets_job = dagster.define_asset_job(
    name="highway_network_job", selection=_filter_for_non_source_assets(highway_network_assets)
)

# tap assets
tap_assets = dagster.load_assets_from_package_module(tap, group_name="tap")
tap_assets_job = dagster.define_asset_job(
    name="tap_job", selection=_filter_for_non_source_assets(tap_assets)
)

# all assets
all_assets = [*demand_assets, *highway_network_assets, *tap_assets]
all_assets_job = dagster.define_asset_job(
    name="all", selection=_filter_for_non_source_assets(all_assets)
)

intermediate_path = str(CACHE_PATH / INTERMEDIATE_PATH)
defs = dagster.Definitions(
    assets=all_assets,
    jobs=[demand_assets_job, highway_network_assets_job, tap_assets_job, all_assets_job],
    resources={
        "default_io_manager": dagster.FilesystemIOManager(base_dir=str(CACHE_PATH)),
        "default_io_manager_intermediate_path": dagster.FilesystemIOManager(
            base_dir=intermediate_path
        ),
        "custom_io_manager": TabularDataLocalIOManager(),
    },
)
