# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest

from openstef_dbc.utils import round_down_time_differences


class TestRoundDownTimeDifferences(unittest.TestCase):
    def test_round_to_smaller_option(self):
        time_diffs = [0.25, 23, 47.4]
        time_options = [0, 1, 24, 48]
        expected_result = [0, 1, 24]
        self.assertEqual(
            round_down_time_differences(time_diffs, time_options), expected_result
        )

    def test_no_bigger_option(self):
        time_diffs = [50, 100]
        time_options = [0, 1, 24, 48]
        expected_result = [48, 48]
        self.assertEqual(
            round_down_time_differences(time_diffs, time_options), expected_result
        )

    def test_no_smaller_option(self):
        time_diffs = [50, 100]
        time_options = [60]
        expected_result = [None, 60]
        self.assertEqual(
            round_down_time_differences(time_diffs, time_options), expected_result
        )

    def test_empty_options(self):
        time_diffs = [1500, 2500, 3000]
        time_options = []
        expected_result = [None, None, None]
        self.assertEqual(
            round_down_time_differences(time_diffs, time_options), expected_result
        )

    def test_options_unsorted(self):
        time_diffs = [0.25, 23, 47.4]
        time_options = [48, 0, 24, 1]
        expected_result = [0, 1, 24]
        self.assertEqual(
            round_down_time_differences(time_diffs, time_options), expected_result
        )


if __name__ == "__main__":
    unittest.main()
