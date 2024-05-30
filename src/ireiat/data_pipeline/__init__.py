import dagster

from .assets import demand, highway_network
from .io_manager import TabularDataLocalIOManager
from ..config import CACHE_PATH

# demand
demand_assets = dagster.load_assets_from_package_module(demand, group_name="demand")
non_source_demand_assets = [a for a in demand_assets if not isinstance(a, dagster.SourceAsset)]
demand_assets_job = dagster.define_asset_job(
    name="highway_demand_job", selection=non_source_demand_assets
)

# highway network
highway_network_assets = dagster.load_assets_from_package_module(
    highway_network, group_name="highway_network"
)
non_source_highway_network_assets = [
    a for a in highway_network_assets if not isinstance(a, dagster.SourceAsset)
]
highway_network_assets_job = dagster.define_asset_job(
    name="highway_network_job", selection=non_source_highway_network_assets
)

defs = dagster.Definitions(
    assets=[*demand_assets, *highway_network_assets],
    jobs=[demand_assets_job, highway_network_assets_job],
    resources={
        "default_io_manager": dagster.FilesystemIOManager(base_dir=str(CACHE_PATH)),
        "custom_io_manager": TabularDataLocalIOManager(),
    },
)
