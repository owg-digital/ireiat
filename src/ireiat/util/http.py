import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import List
import os

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)


def download_file(url: str, save_path: Path | str, force: bool = False) -> bool:
    """Simple download helper to stream the contents of `url` to `save_path`.
    If `save_path` does not exist (including parent directories), it will be created.
    """

    # assume save_path is a fully qualified filename x/y/z.ending
    if isinstance(save_path, str):
        save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)

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
    """Downloads multiple files in a thread pool and saves to disk"""
    for some_path in save_paths:
        Path(os.path.dirname(some_path)).mkdir(parents=True, exist_ok=True)
    download_single_with_force = partial(download_file, force=force)
    print(urls, save_paths)
    with ThreadPoolExecutor() as executor:
        executor.map(download_single_with_force, urls, save_paths)
