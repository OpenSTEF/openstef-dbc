from typing import Union
from datetime import datetime, timedelta

from pydantic import BaseSettings
from pandas import Timestamp
import pandas as pd
import unittest

from openstef_dbc.database import DataBase

from tests.integration.mock_inlfux_db_admin import MockInfluxDBAdmin


class Settings(BaseSettings):
    api_username: str = "test"
    api_password: str = "demo"
    api_admin_username: str = "test"
    api_admin_password: str = "demo"
    api_url: str = "localhost"
    docker_influxdb_init_org: str = "myorg"
    docker_influxdb_init_admin_token: str = "tokenonlyfortesting"
    influx_organization: str = "myorg"
    influxdb_token: str = "tokenonlyfortesting"
    influxdb_host: str = "http://localhost"
    influxdb_port: str = "8086"
    mysql_username: str = "test"
    mysql_password: str = "test"
    mysql_host: str = "localhost"
    mysql_port: int = 1234
    mysql_database_name: str = "test"
    proxies: Union[dict[str, str], None] = None


class TestDataBase(unittest.TestCase):
    def setUp(self) -> None:
        config = Settings()

        self.database = DataBase(config)

        mock_influxdb_admin = MockInfluxDBAdmin(config)
        # Reset to starting conditions
        mock_influxdb_admin.reset_mock_influx_db()

        return super().setUp()

    def test_write_and_read_forecast(self):
        # Arange
        mock_forecast = pd.DataFrame.from_dict(
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
        mock_forecast.index = mock_forecast.index.rename("datetimeFC")

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
        # 0) Test that verifies that it is possible to repeatedly read and write valid dataframes to the database (at least two repetitions)
        pass

    def test_write_read_with_nans(self):
        # 1) Test that verifies that NaN's will not corrupt the shard because of safeguards in write_forecast
        #   - Try to write data twice in a row on the same shard: call write_forecast twice.
        #       - The first time include nan's
        #       - The second time do not include nan's
        #   - assert that the first time no data was written to the database because the data included nan's
        #   - assert that the second time data was successfully written to the database by showing that it can be retrieved
        pass

    def test_write_read_with_wrong_datatype(self):
        # 2) Test that verifies that an accidental wrong datatypes will not corrupt the shard because of safeguards in write_forecast
        #   - Try to write data twice in a row on the same shard: call write_forecast twice.
        #       - The first time, replace some of the values in the dataframe with the wrong datatype
        #       - The second time try to write a valid dataframe
        #   - assert that the first time no data was written to the database because the data included unexpected datatypes
        #   - assert that the second time data was successfully written to the database by showing that it can be retrieved
        #   - As a sanity check: verify that you get a partial write error when you use openstef-dbc 3.7
        pass

    def test_write_read_after_database_restart(self):
        # Comment from Martijn: not sure if this test is essential
        # 3) Test that after a restart of the database it is still possible to write data successfully
        #    - write a valid dataframe to the database
        #    - restart database
        #    - write another valid dataframe to the database
        #    - assert that all data that has been written before and after restart can be retrieved from the database
        pass

    def test_write_read_around_change_of_shard(self):
        # Question from Martijn: When is the change of shard? Is this always the same moment in the week? Do we have a source that describes this?
        # - write valid data just before the change of shard
        # - write valid data just after the change of shard
        # - verify that all data can be retrieved
        pass

    def test_write_read_around_change_of_shard_wrong_datatype(self):
        # - write valid data just before the change of shard
        # - write data which containts some wrong data types just after the change of shard
        # - try writing valid data on the new shard
        # - Assert that all valid data was written to/can be retrieved from the database and that the data with wrong data types was not written to the database
        # - As a sanity check: verify that you would get a partial write error when you use openstef-dbc 3.7
        pass


if __name__ == "__main__":
    unittest.main()
