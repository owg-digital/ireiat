import unittest

from ireiat.data_pipeline.io_manager import _get_read_function


class TestRead(unittest.TestCase):

    def test_read_func_throws_for_unusual_file_format(self):
        with self.assertRaises(NotImplementedError):
            _get_read_function("weird_file_extension")
