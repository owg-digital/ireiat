import functools
import logging
import os
from pathlib import Path
from typing import Callable, Optional

CACHE_PATH = Path(os.getenv("HOMEPATH", "~")) / ".irieat"

logger = logging.getLogger(__name__)


def _cache_read(cache_location: Path):
    """
    Private method for reading supported files from the file system
    :param cache_location:
    :return: opened file for supported types
    """
    file_ending = cache_location.name.split(".")[-1]
    if file_ending == "txt":
        with open(file_ending, "r") as fp:
            result = fp.read()
    else:
        raise NotImplementedError(f"No cache read implemented for {file_ending} files!")
    return result


def _cache_write(obj, cache_destination: Path) -> None:
    file_ending = cache_destination.name.split(".")[-1]
    print(f"Cacheing result at {cache_destination}...")
    if file_ending == "txt":
        with open(cache_destination, "w") as fp:
            fp.write(obj)
    else:
        raise NotImplementedError(
            f"No cache write implemented for {file_ending} files!"
        )


def cacheable(
    cache_filename: str, cache_subdir: Optional[Path] = None, cache_read: bool = True
) -> Callable:
    """
    A decorator meant to store intermediate files in a local cache. Cacheable functions should return
    open objects.

    Existence on the file system is checked, and, if present, the opened file is returned.
    If not present, the function is executed, the result cached, and the result returned.
    :param cache_filename: filename
    :param cache_subdir: subdirectory, if any, to
    :param cache_read: bool to read the file
    :return:
    """
    target_dir = CACHE_PATH / cache_subdir if cache_subdir else CACHE_PATH
    target_dir.mkdir(parents=True, exist_ok=True)
    cacheable_path = target_dir / cache_filename

    def decorator_cacheable(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if cacheable_path.exists():
                logger.info(f"File {cacheable_path} exists - skipping execution.")
                return _cache_read(cacheable_path) if cache_read else None
            result = func(*args, **kwargs)
            _cache_write(result, cacheable_path)
            return result

        return wrapper

    return decorator_cacheable
