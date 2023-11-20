import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import List

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)


def download_file(url: str, save_path: Path, force: bool = False) -> bool:
    # Streaming, so we can iterate over the response.
    if not force:
        if save_path.exists():
            logger.info(f"File {save_path} exists - skipping download.")
            return True
    response = requests.get(url, stream=True, verify=False)
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 kb
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(save_path, "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    return True


def download_files(urls: List[str], save_paths: List[Path], force: bool = False):
    download_single_with_force = partial(download_file, force=force)
    with ThreadPoolExecutor() as executor:
        executor.map(download_single_with_force, urls, save_paths)
