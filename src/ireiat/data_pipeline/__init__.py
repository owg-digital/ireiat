import dagster

from .assets import demand
from .io_manager import TabularDataLocalIOManager
from ..config import CACHE_PATH

faf5_assets = dagster.load_assets_from_package_module(demand)

defs = dagster.Definitions(
    assets=faf5_assets,
    resources={
        "default_io_manager": dagster.FilesystemIOManager(base_dir=str(CACHE_PATH)),
        "custom_io_manager": TabularDataLocalIOManager(),
        "mem_io_manager": dagster.InMemoryIOManager(),
    },
)
