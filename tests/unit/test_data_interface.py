# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from openstef_dbc.data_interface import _DataInterface


@patch("openstef_dbc.data_interface.KtpApi", MagicMock())
@patch("openstef_dbc.data_interface.logging", MagicMock())
@patch("openstef_dbc.data_interface.InfluxDBClient", MagicMock())
@patch("openstef_dbc.data_interface.sqlalchemy", MagicMock())
class TestDataInterface(unittest.TestCase):
    def test_exec_influx_write(self):
        di = _DataInterface()
        n = float("nan")
        # columns a, c contain nan
        df = pd.DataFrame({"a": [1, 2, n], "b": [3, 4, 5], "c": [n, 6, 7]})

        # check ValuError on non list tag_columns argument
        with self.assertRaises(ValueError):
            di.exec_influx_write(df, "testdb", "testmeasurement", "tag1")

        # check ValuError on empty tag_columns list
        with self.assertRaises(ValueError):
            di.exec_influx_write(df, "testdb", "testmeasurement", [])

        # check ValueError on NaN values
        with self.assertRaises(ValueError) as cm:
            di.exec_influx_write(df, "testdb", "testmeasurement", ["tag1"])

        # check if exception msg mentions the columns with NaN'
        self.assertTrue(str(["a", "c"]) in str(cm.exception))

    def test_get_instance(self):
        data_interface_1 = _DataInterface()
        data_interface_2 = _DataInterface.get_instance()
        # should be the same instance
        self.assertIs(data_interface_1, data_interface_2)

    @patch("openstef_dbc.Singleton.get_instance", side_effect=KeyError)
    def test_get_instance_error(self, get_instance_mock):
        with self.assertRaises(RuntimeError):
            _DataInterface.get_instance()


if __name__ == "__main__":
    unittest.main()
