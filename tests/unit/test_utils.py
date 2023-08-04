# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest

from openstef_dbc.utils import round_time_differences, max_timediff


class TestRoundTimeDifferences(unittest.TestCase):
    def test_round_to_bigger_option(self):
        time_diffs = [1500, 2500, 3000]
        time_options = [600, 900, 1800, 3600]
        expected_result = [1800, 3600, 3600]
        self.assertEqual(
            round_time_differences(time_diffs, time_options), expected_result
        )

    def test_no_bigger_option(self):
        time_diffs = [4000, 5000, 6000]
        time_options = [600, 900, 1800, 3600]
        expected_result = [max_timediff, max_timediff, max_timediff]
        self.assertEqual(
            round_time_differences(time_diffs, time_options), expected_result
        )

    def test_empty_options(self):
        time_diffs = [1500, 2500, 3000]
        time_options = []
        expected_result = [max_timediff, max_timediff, max_timediff]
        self.assertEqual(
            round_time_differences(time_diffs, time_options), expected_result
        )


if __name__ == "__main__":
    unittest.main()
