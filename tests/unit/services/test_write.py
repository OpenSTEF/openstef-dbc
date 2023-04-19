## Helper functinos to test write.py.
# All tests are disabled, but can be used to test things manually
# In that case, make a connection to the datase, provide the correct credentials and run the tests
import os
import unittest
import pandas as pd

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.services.write import Write


# Manually define the correct config - ensure you have the correct credentials in your environment
class Config:
    def __init__(self):
        self.influxdb_host = "http://localhost"
        self.influxdb_port = 8086
        self.influxdb_username = "influx"
        self.influxdb_password = os.get("influx_db_password")
        self.influx_organization = "test_org"

        # can be anything, as long as its an int
        self.mysql_port = 3307

    def __getattr__(self, item):
        return None

    @unittest.skip()
    def test_write_taheads():
        """Used to manually test if writing taheads went okay during invalid field dtype period"""
        # Setup connection
        _DataInterface(config=Config())

        # Lets go!
        writer = Write()
        # single line dataframe:
        test_df = pd.DataFrame(
            index=pd.to_datetime(["2023-03-31 17:00:00+00:00"]),
            data={
                "tAhead": 1.0,
                "pid": 307,
                "forecast": 100.0,
                "stdev": 3,
                "customer": "test",
                "description": "test",
                "type": "demand",
                "quality": "invalid",
            },
        )
        # See if it works
        writer._write_t_ahead_series(test_df)
