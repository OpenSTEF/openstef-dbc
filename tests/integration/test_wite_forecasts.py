from typing import Union

from pydantic import BaseSettings
from pandas import Timestamp
import pandas as pd

from openstef_dbc.database import DataBase

from tests.integration.mock_inlfux_db_admin import MockInfluxDBAdmin


class Settings(BaseSettings):
    api_username: str = "test"
    api_password: str = "demo"
    api_admin_username: str = "test"
    api_admin_password: str = "demo"
    api_url: str = "localhost"
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


config = Settings()

database = DataBase(config)

mock_influxdb_admin = MockInfluxDBAdmin(config)
# Reset to starting conditions
mock_influxdb_admin.reset_mock_influx_db()


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

database.write_forecast(mock_forecast)
