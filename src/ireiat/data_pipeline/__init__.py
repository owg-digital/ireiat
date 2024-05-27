from dagster import Definitions, FilesystemIOManager

from ireiat.data_pipeline.assets.faf5_to_county import faf_id_to_county_areas
from ireiat.data_pipeline.assets.sources import faf5_regions
from .io_manager import TabularDataLocalIOManager
from ..config import CACHE_PATH

defs = Definitions(
    assets=[faf5_regions, faf_id_to_county_areas],
    resources={
        "default_io_manager": FilesystemIOManager(base_dir=str(CACHE_PATH)),
        "custom_io_manager": TabularDataLocalIOManager(),
    },
)
