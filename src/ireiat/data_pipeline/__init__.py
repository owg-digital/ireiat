import dagster

from .assets import demand, highway_network, tap, rail_network, marine_network
from .io_manager import TabularDataLocalIOManager
from ..config import CACHE_PATH, INTERMEDIATE_PATH

# demand
demand_assets = dagster.load_assets_from_package_module(demand, group_name="demand")
demand_assets_job = dagster.define_asset_job(name="demand_job")

# highway network
highway_network_assets = dagster.load_assets_from_package_module(
    highway_network, group_name="highway_network"
)
highway_network_assets_job = dagster.define_asset_job(name="highway_network_job")

# rail network
rail_network_assets = dagster.load_assets_from_package_module(
    rail_network, group_name="rail_network"
)
rail_network_assets_job = dagster.define_asset_job(
    name="rail_network_job",
)

# marine network
marine_network_assets = dagster.load_assets_from_package_module(
    marine_network, group_name="marine_network"
)
marine_network_assets_job = dagster.define_asset_job(name="marine_network_job")

# tap assets
tap_assets = dagster.load_assets_from_package_module(tap, group_name="tap")
tap_assets_job = dagster.define_asset_job(name="tap_job")

# all assets
all_assets = [
    *demand_assets,
    *highway_network_assets,
    *rail_network_assets,
    *marine_network_assets,
    *tap_assets,
]
all_assets_job = dagster.define_asset_job(name="all")

intermediate_path = str(CACHE_PATH / INTERMEDIATE_PATH)
defs = dagster.Definitions(
    assets=all_assets,
    jobs=[
        demand_assets_job,
        highway_network_assets_job,
        rail_network_assets_job,
        marine_network_assets_job,
        tap_assets_job,
        all_assets_job,
    ],
    resources={
        "default_io_manager": dagster.FilesystemIOManager(base_dir=str(CACHE_PATH)),
        "default_io_manager_intermediate_path": dagster.FilesystemIOManager(
            base_dir=intermediate_path
        ),
        "custom_io_manager": TabularDataLocalIOManager(),
    },
)
