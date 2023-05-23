# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import geopy
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import requests
import sqlalchemy

from openstef_dbc import Singleton
from openstef_dbc.ktp_api import KtpApi
from openstef_dbc.log import logging

# Define abstract interface


class _DataInterface(metaclass=Singleton):
    def __init__(self, config):
        """Generic data interface.

        All connections and queries to the InfluxDB database, MySQL databases and
        influx API are governed by this class.

        Args:
            config: Configuration object. with the following attributes:
                api_username (str): API username.
                api_password (str): API password.
                api_admin_username (str): API admin username.
                api_admin_password (str): API admin password.
                api_url (str): API url.
                influxdb_token (str): Token to authenticate to InfluxDB.
                influxdb_host (str): InfluxDB host.
                influxdb_port (int): InfluxDB port.
                influx_organization (str): InfluxDB organization.
                mysql_username (str): MySQL username.
                mysql_password (str): MySQL password.
                mysql_host (str): MySQL host.
                mysql_port (int): MYSQL port.
                mysql_database_name (str): MySQL database name.
                proxies Union[dict[str, str], None]: Proxies.
        """

        self.logger = logging.get_logger(self.__class__.__name__)
        self.influx_organization = config.influx_organization

        self.ktp_api = KtpApi(
            username=config.api_username,
            password=config.api_password,
            admin_username=config.api_admin_username,
            admin_password=config.api_admin_password,
            url=config.api_url,
            proxies=config.proxies,
        )

        self.influx_client = self._create_influx_client(
            token=config.influxdb_token,
            host=config.influxdb_host,
            port=config.influxdb_port,
            organization=config.influx_organization,
        )

        self.influx_query_api = self.influx_client.query_api()
        self.influx_write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

        self.mysql_engine = self._create_mysql_engine(
            username=config.mysql_username,
            password=config.mysql_password,
            host=config.mysql_host,
            port=config.mysql_port,
            db=config.mysql_database_name,
        )

        # Set geopy proxies
        # https://geopy.readthedocs.io/en/stable/#geopy.geocoders.options
        # https://docs.python.org/3/library/urllib.request.html#urllib.request.ProxyHandler
        # By default the system proxies are respected
        # (e.g. HTTP_PROXY and HTTPS_PROXY env vars or platform-specific proxy settings,
        # such as macOS or Windows native preferences – see
        # urllib.request.ProxyHandler for more details).
        # The proxies value for using system proxies is None.
        geopy.geocoders.options.default_proxies = config.proxies

        _DataInterface._instance = self

    @staticmethod
    def get_instance():
        try:
            return Singleton.get_instance(_DataInterface)
        except KeyError as exc:
            # if _DataInterface not in Singleton._instances:
            raise RuntimeError(
                "No _DataInterface instance initialized. "
                "Please call _DataInterface(config) first."
            ) from exc

    def _create_influx_client(
        self, token: str, host: str, port: int, organization: str
    ) -> None:
        """Create influx client, namespace-dependend"""
        try:
            return InfluxDBClient(
                url=f"{host}:{port}",
                token=token,
                org=organization,
                timeout=30_000,
            )
        except Exception as exc:
            self.logger("Could not connect to InfluxDB database", exc_info=exc)
            raise

    def _create_mysql_engine(
        self, username: str, password: str, host: str, port: int, db: str
    ):
        """Create MySQL engine.

        Differs from sql_connection in the sense that this write_engine
        *can* write pandas dataframe directly.

        """
        connector = "mysql+mysqlconnector"
        database_url = (
            f"{connector}://{username}:{password}@{host}:{port}/{db}?use_pure=True"
        )
        try:
            return sqlalchemy.create_engine(database_url)
        except Exception as exc:
            self.logger.error("Could not connect to MySQL database", exc_info=exc)
            raise

    def exec_influx_query(self, query: str, bind_params: dict = {}) -> dict:
        """Execute an InfluxDB query.

        When there is data it returns a defaultdict with as key the measurement and
        as value a DataFrame. When there is NO data it returns an empty dictionairy.

        Args:
            query (str): Influx query string.
            bind_params (dict): Binding parameter for parameterized queries

        Returns:
            defaultdict: Query result.
        """
        try:
            return self.influx_query_api.query_data_frame(query)
        except requests.exceptions.ConnectionError as e:
            self.logger.error("Lost connection to InfluxDB database", exc_info=e)
            raise
        except Exception as e:
            self.logger.error(
                "Error occured during executing InfluxDB query", query=query, exc_info=e
            )
            raise

    def exec_influx_write(
        self,
        df: pd.DataFrame,
        database: str,
        measurement: str,
        tag_columns: list,
        organization: str = None,
        field_columns: list = None,
        time_precision: str = "s",
    ) -> bool:
        if field_columns is None:
            field_columns = []
        if type(tag_columns) is not list:
            raise ValueError("'tag_columns' should be a list")
        if organization is None:
            organization = self.influx_organization

        if len(tag_columns) == 0:
            raise ValueError("At least one tag column should be given in 'tag_columns'")

        # Check if a value is nan
        if True in df.isna().values:
            nan_columns = df.columns[df.isna().any()].tolist()
            raise ValueError(
                f"Dataframe contains NaN's. Found NaN's in columns: {nan_columns}"
            )
        # Check if a value is inf
        if True in df.isinf().values:
            nan_columns = df.columns[df.isinf().any()].tolist()
            raise ValueError(
                f"Dataframe contains Inf's. Found Inf's in columns: {nan_columns}"
            )

        if True in df.isnull().values:
            nan_columns = df.columns[df.isinf().any()].tolist()
            raise ValueError(
                f"Dataframe contains missing values. Found missing values in columns: {nan_columns}"
            )

        try:
            self.influx_write_api.write(
                record=df,
                data_frame_measurement_name=measurement,
                bucket=f"{database}/autogen",
                data_frame_tag_columns=tag_columns,
                record_field_keys=field_columns,
                write_precision=time_precision,
                org=organization,
            )
            return True
        except Exception as e:
            self.logger.error(
                "Exception occured during writing to InfluxDB", exc_info=e
            )
            raise

    def check_influx_available(self):
        """Check if a basic influx query gives a valid response"""
        query = "buckets()"
        response = self.exec_influx_query(query)
        if isinstance(response, pd.DataFrame):
            available = not response.empty
        else:
            available = False

        return available

    def exec_sql_query(self, query: str, params: dict = None, **kwargs):
        if params is None:
            params = {}
        try:
            return pd.read_sql(query, self.mysql_engine, params=params, **kwargs)
        except sqlalchemy.exc.OperationalError as e:
            self.logger.error("Lost connection to MySQL database", exc_info=e)
            raise
        except sqlalchemy.exc.ProgrammingError as e:
            self.logger.error(
                "Error occured during executing query", query=query, exc_info=e
            )
            raise
        except sqlalchemy.exc.DatabaseError as e:
            self.logger.error("Can't connect to MySQL database", exc_info=e)
            raise

    def exec_sql_write(self, statement: str, params: dict = None) -> None:
        if params is None:
            params = {}
        try:
            with self.mysql_engine.connect() as connection:
                connection.execute(statement, params=params)
        except Exception as e:
            self.logger.error(
                "Error occured during executing query", query=statement, exc_info=e
            )
            raise

    def exec_sql_dataframe_write(
        self, dataframe: pd.DataFrame, table: str, **kwargs
    ) -> None:
        dataframe.to_sql(table, self.mysql_engine, **kwargs)

    def check_mysql_available(self):
        """Check if a basic mysql query gives a valid response"""
        query = "SHOW DATABASES"
        response = self.exec_sql_query(query)

        available = len(list(response["Database"])) > 0

        return available
