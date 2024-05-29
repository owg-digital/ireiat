import dagster

from .assets import demand
from .io_manager import TabularDataLocalIOManager
from ..config import CACHE_PATH

demand_assets = dagster.load_assets_from_package_module(demand, group_name="demand")
non_source_demand_assets = [a for a in demand_assets if not isinstance(a, dagster.SourceAsset)]
demand_assets_job = dagster.define_asset_job(
    name="highway_demand_job", selection=non_source_demand_assets
)
defs = dagster.Definitions(
    assets=demand_assets,
    jobs=[demand_assets_job],
    resources={
        "default_io_manager": dagster.FilesystemIOManager(base_dir=str(CACHE_PATH)),
        "custom_io_manager": TabularDataLocalIOManager(),
        "mem_io_manager": dagster.InMemoryIOManager(),
    },
)
