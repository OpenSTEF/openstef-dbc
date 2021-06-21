import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from ktpbase.data_interface import _DataInterface


@patch("openstf_dbc.data_interface.ConfigManager", MagicMock())
@patch("openstf_dbc.data_interface.KtpApi", MagicMock())
@patch("openstf_dbc.data_interface.logging", MagicMock())
@patch("openstf_dbc.data_interface.influxdb", MagicMock())
@patch("openstf_dbc.data_interface.sqlalchemy", MagicMock())
class TestDataInterface(unittest.TestCase):

    def test_exec_influx_write(self):
        di = _DataInterface.get_instance()
        n = float("nan")
        # columns a, c contain nan
        df = pd.DataFrame(
            {"a": [1, 2, n], "b": [3, 4, 5], "c": [n, 6, 7]}
        )

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


if __name__ == "__main__":
    unittest.main()
