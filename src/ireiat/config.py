from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, List
import os
import yaml

CACHE_PATH = Path(os.getenv("HOMEPATH", "~")) / ".ireiat"
RADIUS_EARTH_MILES = 3958.8
TOLERANCE = 1e-5
LATLONG_CRS = "EPSG:4326"
ALBERS_CRS = "EPSG:5070"
HIGHWAY_CAPACITY_TONS = (
    80000  # TODO (NP): Placeholder while we translate from tons->vehicles or put a tonnage limit
)
HIGHWAY_BETA = 4  # the exponent in the congestion term
HIGHWAY_ALPHA = 0.15  # the scalar in the congestion term

# exclude some states, regions from the input data
EXCLUDED_FIPS_CODES_MAP = {
    "Alaska": "02",
    "Hawaii": "15",
    "Puerto Rico": "72",
    "American Samoa": "60",
    "Northern Mariana Islands": "69",
    "Guam": "66",
    "US Virgin Islands": "78",
}


@dataclass
class Config:
    raw_files: Dict[str, str]

    @property
    def download_targets(self) -> Tuple[List[Path], List[str]]:
        raw_target_fnames, raw_urls = zip(*self.raw_files.items())
        return [CACHE_PATH / "data/raw" / f for f in raw_target_fnames], list(raw_urls)

    @classmethod
    def from_yaml(cls, yaml_file_path: Path = Path("config.yml")):
        with open(yaml_file_path) as fp:
            result = yaml.safe_load(fp)
        return cls(**result)
