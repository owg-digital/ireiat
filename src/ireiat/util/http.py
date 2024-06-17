import datetime
import logging
from pathlib import Path

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)


def download_uncached_file(url: str, save_path: Path | str, force: bool = False) -> bool:
    """Simple download helper to stream the contents of `url` to `save_path`.
    If `save_path` does not exist (including parent directories) on the filesystem, it will be created.

    After downloading, this method creates a "{save_path}.sentinel" file to indicate that the
    download was complete. If the sentinel file is not present, the download is assumed
    to have been partial and the file is re-downloaded.
    """

    # assume save_path is a fully qualified filename x/y/z.ending
    if isinstance(save_path, str):
        save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    sentinel_file_path = save_path.parent / f"{save_path.name}.sentinel"

    if not force:
        if save_path.exists():
            if sentinel_file_path.exists():
                logger.info(f"File {save_path} and sentinel exists - skipping download.")
                return True
            else:
                logger.info("Sentinel not found! Re-downloading file...")

    # remove the file and its sentinel
    save_path.unlink(missing_ok=True)
    sentinel_file_path.unlink(missing_ok=True)
    response = requests.get(
        url, stream=True, verify=False, timeout=10
    )  # 10 second timeout for connect or read
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 kb
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(save_path, "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    with open(sentinel_file_path, "w") as fp:
        fp.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return True
