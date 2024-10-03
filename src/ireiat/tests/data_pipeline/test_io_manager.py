import tempfile
import unittest
from pathlib import Path
from unittest.mock import create_autospec

import dagster
import pandas as pd

from ireiat.config.constants import CACHE_PATH
from ireiat.data_pipeline.io_manager import (
    _get_read_function,
    _get_fs_path,
    read_or_attempt_download,
)
from ireiat.util.http import download_uncached_file


class TestIOManager(unittest.TestCase):

    def setUp(self):
        self.dummy_df = pd.DataFrame([1, 2, 3], columns=["dummy"])

    def test_read_func_throws_for_unusual_file_format(self):
        with self.assertRaises(NotImplementedError):
            _get_read_function("weird_file_extension")

    def test_src_path_resolves(self):
        result = _get_fs_path(dagster.AssetKey("dummy"), {"format": "dummy_extension"})
        self.assertIsInstance(result, str)
        self.assertEqual(Path(result), (CACHE_PATH / "dummy.dummy_extension").absolute())

        # specifying filename overrides any file format
        result = _get_fs_path(
            dagster.AssetKey("dummy"), {"filename": "new_dummy.blah", "format": "dummy_extension"}
        )
        self.assertEqual(Path(result), (CACHE_PATH / "new_dummy.blah").absolute())

    def test_file_read_from_filesystem_when_file_exists(self):
        temp_fname = "test"
        temp_extension = "csv"
        with tempfile.TemporaryDirectory() as td:
            temp_dir_name = Path(td).parent
            self.dummy_df.to_csv(temp_dir_name / f"{temp_fname}.{temp_extension}")
            result = read_or_attempt_download(
                dagster.AssetKey(temp_fname),
                {"source_path": temp_dir_name, "format": temp_extension},
            )
            self.assertIsInstance(result, pd.DataFrame)

    def test_throws_error_when_file_does_not_exist_and_no_url(self):
        with self.assertRaises(IOError):
            read_or_attempt_download(dagster.AssetKey("bad_file"), {"format": "csv"})

    def test_tries_to_download_when_url_specified(self):
        mock_download_func = create_autospec(download_uncached_file, return_value=True)
        try:
            asset_key = dagster.AssetKey("bad_file")
            metadata = {"format": "csv", "url": "badurl"}
            fpath = _get_fs_path(asset_key, metadata)
            read_or_attempt_download(asset_key, metadata)
            mock_download_func.assert_called_once_with("badurl", fpath)
        except IOError:
            pass
