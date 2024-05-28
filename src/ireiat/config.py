from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, List
import os
import yaml

CACHE_PATH = Path(os.getenv("HOMEPATH", "~")) / ".irieat"


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
