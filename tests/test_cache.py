import unittest

from ireiat.util.cacheable import cacheable, CACHE_PATH


class CacheableTests(unittest.TestCase):
    def test_cache_writes_a_file_correctly(self):
        def do_something_expensive(num: int):
            return f"expensive result {num}"

        target_cache_file = "expensive_cached_result.txt"
        cacheable(cache_filename=target_cache_file, cache_read=False)(
            do_something_expensive
        )(num=3)
        expected_cache_location = CACHE_PATH / target_cache_file
        self.assertTrue(expected_cache_location.exists())
        expected_cache_location.unlink()


if __name__ == "__main__":
    unittest.main()
