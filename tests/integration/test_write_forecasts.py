# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from typing import Union
from datetime import datetime, timedelta

from pydantic import BaseSettings
from pandas import Timestamp
import pandas as pd
import numpy as np
import unittest
import warnings

from openstef_dbc.database import DataBase

from tests.integration.mock_influx_db_admin import MockInfluxDBAdmin

from tests.integration.settings import Settings


class TestDataBase(unittest.TestCase):
    def setUp(self) -> None:
        # Initialize settings
        config = Settings()

        print(config.dict())
        # Inizitalize Influx admin controller
        mock_influxdb_admin = MockInfluxDBAdmin(config)

        # if not mock_influxdb_admin.is_available():
        #     warnings.warn("InfluxDB instance not found, skipping integration tests.")
        #     raise unittest.SkipTest(
        #         "InfluxDB instance not found, skipping integration tests."
        #     )

        # Initialize database object
        self.database = DataBase(config)

        # Reset influxDB to starting conditions
        mock_influxdb_admin.reset_mock_influx_db()

        self.mock_forecast = pd.DataFrame.from_dict(
            {
                "forecast": {
                    Timestamp("2022-01-01 00:00:00"): 0.0,
                    Timestamp("2022-01-01 00:15:00"): 0.1,
                    Timestamp("2022-01-01 00:30:00"): 0.0,
                    Timestamp("2022-01-01 00:45:00"): 0.1,
                },
                "stdev": {
                    Timestamp("2022-01-01 00:00:00"): 0.0,
                    Timestamp("2022-01-01 00:15:00"): 0.1,
                    Timestamp("2022-01-01 00:30:00"): 0.0,
                    Timestamp("2022-01-01 00:45:00"): 0.1,
                },
                "quality": {
                    Timestamp("2022-01-01 00:00:00"): "actual",
                    Timestamp("2022-01-01 00:15:00"): "actual",
                    Timestamp("2022-01-01 00:30:00"): "actual",
                    Timestamp("2022-01-01 00:45:00"): "actual",
                },
                "quantile_P50": {
                    Timestamp("2022-01-01 00:00:00"): 0.0,
                    Timestamp("2022-01-01 00:15:00"): 0.1,
                    Timestamp("2022-01-01 00:30:00"): 0.0,
                    Timestamp("2022-01-01 00:45:00"): 0.1,
                },
                "forecast_wind_on_shore": {
                    Timestamp("2022-01-01 00:00:00"): 0.0,
                    Timestamp("2022-01-01 00:15:00"): 0.1,
                    Timestamp("2022-01-01 00:30:00"): 0.0,
                    Timestamp("2022-01-01 00:45:00"): 0.1,
                },
                "forecast_solar": {
                    Timestamp("2022-01-01 00:00:00"): 0.0,
                    Timestamp("2022-01-01 00:15:00"): 0.1,
                    Timestamp("2022-01-01 00:30:00"): 0.0,
                    Timestamp("2022-01-01 00:45:00"): 0.1,
                },
                "forecast_other": {
                    Timestamp("2022-01-01 00:00:00"): 0.0,
                    Timestamp("2022-01-01 00:15:00"): 0.1,
                    Timestamp("2022-01-01 00:30:00"): 0.0,
                    Timestamp("2022-01-01 00:45:00"): 0.1,
                },
                "pid": {
                    Timestamp("2022-01-01 00:00:00"): 308,
                    Timestamp("2022-01-01 00:15:00"): 308,
                    Timestamp("2022-01-01 00:30:00"): 308,
                    Timestamp("2022-01-01 00:45:00"): 308,
                },
                "customer": {
                    Timestamp("2022-01-01 00:00:00"): "name",
                    Timestamp("2022-01-01 00:15:00"): "name",
                    Timestamp("2022-01-01 00:30:00"): "name",
                    Timestamp("2022-01-01 00:45:00"): "name",
                },
                "description": {
                    Timestamp("2022-01-01 00:00:00"): "TEST",
                    Timestamp("2022-01-01 00:15:00"): "TEST",
                    Timestamp("2022-01-01 00:30:00"): "TEST",
                    Timestamp("2022-01-01 00:45:00"): "TEST",
                },
                "type": {
                    Timestamp("2022-01-01 00:00:00"): "demand",
                    Timestamp("2022-01-01 00:15:00"): "demand",
                    Timestamp("2022-01-01 00:30:00"): "demand",
                    Timestamp("2022-01-01 00:45:00"): "demand",
                },
                "algtype": {
                    Timestamp("2022-01-01 00:00:00"): "posted_by_api",
                    Timestamp("2022-01-01 00:15:00"): "posted_by_api",
                    Timestamp("2022-01-01 00:30:00"): "posted_by_api",
                    Timestamp("2022-01-01 00:45:00"): "posted_by_api",
                },
            }
        )
        self.mock_forecast.index = self.mock_forecast.index.rename("datetimeFC")

    def test_write_and_read_forecast(self):
        # Arange
        mock_forecast = self.mock_forecast.copy(deep=True)
        expected_df = pd.DataFrame.from_dict(
            {
                "forecast": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                },
                "stdev": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                },
            }
        )
        expected_df.index = expected_df.index.rename("datetime")
        expected_df.columns.name = ""

        # Act
        self.database.write_forecast(mock_forecast)

        # Assert
        result = self.database.get_predicted_load(
            pj={"id": 308, "resolution_minutes": 15},
            start_time=datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime(2022, 1, 1, 2, 0, 0),
        )
        pd.testing.assert_frame_equal(result, expected_df)

    def test_write_read_with_repeatedly(self):
        """Test that verifies that it is possible to repeatedly read and write valid dataframes to the database (at least two repetitions)"""
        # Arange
        first_mock_forecast = self.mock_forecast.copy(deep=True)
        second_mock_forecast = self.mock_forecast.copy(deep=True).shift(2, freq="15T")

        expected_df = pd.DataFrame.from_dict(
            {
                "forecast": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 01:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 01:15:00+0000", tz="UTC"): 0.1,
                },
                "stdev": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 01:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 01:15:00+0000", tz="UTC"): 0.1,
                },
            }
        )
        expected_df.index = expected_df.index.rename("datetime")
        expected_df.columns.name = ""

        # Act
        self.database.write_forecast(first_mock_forecast)
        self.database.write_forecast(second_mock_forecast)

        # Assert
        result = self.database.get_predicted_load(
            pj={"id": 308, "resolution_minutes": 15},
            start_time=datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime(2022, 1, 1, 3, 0, 0),
        )

        pd.testing.assert_frame_equal(result, expected_df)

    def test_write_read_with_nans(self):
        """
        Test that verifies that NaN's will not corrupt the shard because of safeguards in write_forecast
          - Try to write data twice in a row on the same shard: call write_forecast twice.
              - The first time include nan's
              - The second time do not include nan's
          - assert that the first time no data was written to the database because the data included nan's
          - assert that the second time data was successfully written to the database by showing that it can be retrieved
        """

        # Arange
        first_mock_forecast = self.mock_forecast.copy(deep=True)
        second_mock_forecast = self.mock_forecast.copy(deep=True).shift(2, freq="15T")

        first_mock_forecast.loc["2022-01-01 00:15:00+0000"] = np.nan

        expected_df = pd.DataFrame.from_dict(
            {
                "forecast": {
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 01:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 01:15:00+0000", tz="UTC"): 0.1,
                },
                "stdev": {
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 01:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 01:15:00+0000", tz="UTC"): 0.1,
                },
            }
        )
        expected_df.index = expected_df.index.rename("datetime")
        expected_df.columns.name = ""

        # Act and assert
        with self.assertRaises(ValueError):
            self.database.write_forecast(first_mock_forecast)

        self.database.write_forecast(second_mock_forecast)

        # Assert
        result = self.database.get_predicted_load(
            pj={"id": 308, "resolution_minutes": 15},
            start_time=datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime(2022, 1, 1, 3, 0, 0),
        )

        pd.testing.assert_frame_equal(result, expected_df)

    def test_write_read_with_wrong_datatype(self):
        """Test that verifies that an accidental wrong datatypes will not corrupt the shard because of safeguards in write_forecast
        - Try to write data twice in a row on the same shard: call write_forecast twice.
            - The first time, replace some of the values in the dataframe with the wrong datatype
            - The second time try to write a valid dataframe
        - assert that the first time no data was written to the database because the data included unexpected datatypes
        - assert that the second time data was successfully written to the database by showing that it can be retrieved
        - As a sanity check: verify that you get a partial write error when you use openstef-dbc 3.7
        """

        # Arange
        first_mock_forecast = self.mock_forecast.copy(deep=True)
        second_mock_forecast = self.mock_forecast.copy(deep=True).shift(2, freq="15T")

        first_mock_forecast.loc["2022-01-01 00:15:00+0000"] = "This is not a float"

        expected_df = pd.DataFrame.from_dict(
            {
                "forecast": {
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 01:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 01:15:00+0000", tz="UTC"): 0.1,
                },
                "stdev": {
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 01:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 01:15:00+0000", tz="UTC"): 0.1,
                },
            }
        )
        expected_df.index = expected_df.index.rename("datetime")
        expected_df.columns.name = ""

        # Act and assert
        with self.assertRaises(ValueError):
            self.database.write_forecast(first_mock_forecast)
        self.database.write_forecast(second_mock_forecast)

        # Assert
        result = self.database.get_predicted_load(
            pj={"id": 308, "resolution_minutes": 15},
            start_time=datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime(2022, 1, 1, 3, 0, 0),
        )
        pd.testing.assert_frame_equal(result, expected_df)

    def test_write_read_around_different_shards(self):
        """
        Question from Martijn: When is the change of shard? Is this always the same moment in the week? Do we have a source that describes this?
        - write valid data just before the change of shard
        - write valid data just after the change of shard
        - verify that all data can be retrieved"""
        # Arange
        first_mock_forecast = self.mock_forecast.copy(deep=True)
        second_mock_forecast = self.mock_forecast.copy(deep=True).shift(
            2 * 96, freq="15T"
        )

        expected_df = pd.DataFrame.from_dict(
            {
                "forecast": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-03 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-03 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-03 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-03 00:45:00+0000", tz="UTC"): 0.1,
                },
                "stdev": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-03 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-03 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-03 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-03 00:45:00+0000", tz="UTC"): 0.1,
                },
            }
        )
        expected_df.index = expected_df.index.rename("datetime")
        expected_df.columns.name = ""

        # Act
        self.database.write_forecast(first_mock_forecast)
        self.database.write_forecast(second_mock_forecast)

        # Assert
        result = self.database.get_predicted_load(
            pj={"id": 308, "resolution_minutes": 15},
            start_time=first_mock_forecast.index.min().to_pydatetime()
            - timedelta(days=1),
            end_time=second_mock_forecast.index.max().to_pydatetime()
            + timedelta(days=1),
        )
        pd.testing.assert_frame_equal(result, expected_df)

    def test_write_read_around_change_of_shard_wrong_datatype(self):
        """
        - write valid data just before the change of shard
        - write data which containts some wrong data types just after the change of shard
        - try writing valid data on the new shard
        - Assert that all valid data was written to/can be retrieved from the database and that the data with wrong data types was not written to the database
        - As a sanity check: verify that you would get a partial write error when you use openstef-dbc 3.7
        """
        # Arange
        first_mock_forecast = self.mock_forecast.copy(deep=True)
        second_mock_forecast = self.mock_forecast.copy(deep=True).shift(
            2 * 96, freq="15T"
        )
        second_mock_forecast.loc["2022-01-03 00:15:00+0000"] = "This is not a float"
        third_mock_forecast = self.mock_forecast.copy(deep=True).shift(
            3 * 96, freq="15T"
        )

        expected_df = pd.DataFrame.from_dict(
            {
                "forecast": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-04 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-04 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-04 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-04 00:45:00+0000", tz="UTC"): 0.1,
                },
                "stdev": {
                    Timestamp("2022-01-01 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-01 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-01 00:45:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-04 00:00:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-04 00:15:00+0000", tz="UTC"): 0.1,
                    Timestamp("2022-01-04 00:30:00+0000", tz="UTC"): 0.0,
                    Timestamp("2022-01-04 00:45:00+0000", tz="UTC"): 0.1,
                },
            }
        )
        expected_df.index = expected_df.index.rename("datetime")
        expected_df.columns.name = ""

        # Act
        self.database.write_forecast(first_mock_forecast)
        with self.assertRaises(ValueError):
            self.database.write_forecast(second_mock_forecast)
        self.database.write_forecast(third_mock_forecast)

        # Assert
        result = self.database.get_predicted_load(
            pj={"id": 308, "resolution_minutes": 15},
            start_time=first_mock_forecast.index.min().to_pydatetime()
            - timedelta(days=1),
            end_time=third_mock_forecast.index.max().to_pydatetime()
            + timedelta(days=1),
        )
        pd.testing.assert_frame_equal(result, expected_df)


if __name__ == "__main__":
    unittest.main()
