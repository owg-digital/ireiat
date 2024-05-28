from dagster import Definitions, FilesystemIOManager, load_assets_from_package_module


from . import assets
from .io_manager import TabularDataLocalIOManager
from ..config import CACHE_PATH

faf5_assets = load_assets_from_package_module(assets)

defs = Definitions(
    assets=faf5_assets,
    resources={
        "default_io_manager": FilesystemIOManager(base_dir=str(CACHE_PATH)),
        "custom_io_manager": TabularDataLocalIOManager(),
    },
)
