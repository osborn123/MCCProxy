import unittest

import pandas as pd

from src.main.ML.FeatureExtractor import extract_features, get_num_accesses_last_k_time_steps, \
    get_num_steps_between_accesses, trailing_zeros


class TestFeatureExtractor(unittest.TestCase):
    def setUp(self):
        self.raw_features = pd.DataFrame(
            {'read': [[1], [2], [3], [4], [5]],
             'write': [[6], [7], [8], [9], [10]]})

    def test_extract_features(self):
        result = extract_features(self.raw_features)
        # Assert the shape of the result
        self.assertEqual(result.shape, (5, 24))

    def test_get_num_accesses_last_k_time_steps(self):
        access_array = [1, 2, 4]  # 1, 10, 100 3 * 64 = 192
        self.assertEqual(1, get_num_accesses_last_k_time_steps(access_array, 1))
        self.assertEqual(1, get_num_accesses_last_k_time_steps(access_array, 64))
        self.assertEqual(1, get_num_accesses_last_k_time_steps(access_array, 65))
        self.assertEqual(2, get_num_accesses_last_k_time_steps(access_array, 66))
        self.assertEqual(2, get_num_accesses_last_k_time_steps(access_array, 67))
        self.assertEqual(3, get_num_accesses_last_k_time_steps(access_array, 192))
        self.assertEqual(3, get_num_accesses_last_k_time_steps(access_array, 193))

    def test_get_num_steps_between_accesses(self):
        access_array = [1, 2, 4]  # 1, 10, 100 3 * 64 = 192
        self.assertEqual([1], get_num_steps_between_accesses(access_array, 1))
        self.assertEqual([1, 65], get_num_steps_between_accesses(access_array, 2))
        self.assertEqual([1, 65, 65], get_num_steps_between_accesses(access_array, 3))

    def test_trailing_zeros(self):
        result = trailing_zeros(16)
        # Assert the expected result
        self.assertEqual(result, 4)


if __name__ == '__main__':
    unittest.main()
