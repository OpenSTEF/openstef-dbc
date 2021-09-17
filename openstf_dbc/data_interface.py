# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import geopy
import influxdb
import pandas as pd
import requests
import sqlalchemy

from openstf_dbc import Singleton
from openstf_dbc.ktp_api import KtpApi
from openstf_dbc.log import logging

# Define abstract interface


class _DataInterface(metaclass=Singleton):
    def __init__(self, config):
        """Generic data interface.

        All connections and queries to the InfluxDB database, MySQL databases and
        influx API are governed by this class.

        Args:
            config: Configuration object. with the following attributes:
                api.username (str): API username.
                api.password (str): API password.
                api.admin_username (str): API admin username.
                api.admin_password (str): API admin password.
                api.url (str): API url.
                influxdb.username (str): InfluxDB username.
                influxdb.password (str): InfluxDB password.
                influxdb.host (str): InfluxDB host.
                influxdb.port (int): InfluxDB port.
                mysql.username (str): MySQL username.
                mysql.password (str): MySQL password.
                mysql.host (str): MySQL host.
                mysql.port (int): MYSQL port.
                mysql.database_name (str): MySQL database name.
                proxies Union[dict[str, str], None]: Proxies.
        """

        self.logger = logging.get_logger(self.__class__.__name__)

        self.ktp_api = KtpApi(
            username=config.api.username,
            password=config.api.password,
            admin_username=config.api.admin_username,
            admin_password=config.api.admin_password,
            url=config.api.url,
            proxies=config.proxies,
        )

        self.influx_client = self._create_influx_client(
            username=config.influxdb.username,
            password=config.influxdb.password,
            host=config.influxdb.host,
            port=config.influxdb.port,
        )

        self.mysql_engine = self._create_mysql_engine(
            username=config.mysql.username,
            password=config.mysql.password,
            host=config.mysql.host,
            port=config.mysql.port,
            db=config.mysql.database_name,
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

    def _create_influx_client(self, username, password, host, port):
        """Create influx client, namespace-dependend"""
        try:
            return influxdb.DataFrameClient(
                host=host,
                port=port,
                username=username,
                password=password,
            )
        except Exception as exc:
            self.logger("Could not connect to InfluxDB database", exc_info=exc)
            raise

    def _create_mysql_engine(self, username, password, host, port, db):
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

    def exec_influx_query(self, query):
        """Execute an InfluxDB query.

        When there is data it returns a defaultdict with as key the measurement and
        as value a DataFrame. When there is NO data it returns an empty dictionairy.

        Args:
            query (str): Influx query string.

        Returns:
            defaultdict: Query result.
        """
        try:
            return self.influx_client.query(query, chunked=True, chunk_size=10000)
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
        df,
        database,
        measurement,
        tag_columns,
        field_columns=None,
        time_precision="s",
        protocol="json",
    ):

        if field_columns is None:
            field_columns = []
        if type(tag_columns) is not list:
            raise ValueError("'tag_columns' should be a list")

        if len(tag_columns) == 0:
            raise ValueError("At least one tag column should be given in 'tag_columns'")

        # Check if a value is nan
        if True in df.isna().values:
            nan_columns = df.columns[df.isna().any()].tolist()
            raise ValueError(
                f"Dataframe contains NaN's. Found NaN's in columns: {nan_columns}"
            )

        try:
            self.influx_client.write_points(
                df,
                measurement=measurement,
                database=database,
                tag_columns=tag_columns,
                field_columns=field_columns,
                time_precision=time_precision,
                protocol=protocol,
            )
            return True
        except Exception as e:
            self.logger.error(
                "Exception occured during writing to InfluxDB", exc_info=e
            )
            raise

    def check_influx_available(self):
        """Check if a basic influx query gives a valid response"""
        query = "SHOW DATABASES"
        response = self.exec_influx_query(query)

        available = len(list(response["databases"])) > 0

        return available

    def exec_sql_query(self, query, **kwargs):
        try:
            return pd.read_sql(query, self.mysql_engine, **kwargs)
        except sqlalchemy.exc.OperationalError as e:
            self.logger.error("Lost connection to MySQL database", exc_info=e)
            raise
        except sqlalchemy.exc.ProgrammingError as e:
            self.logger.error(
                "Error occured during executing query", query=query, exc_info=e
            )
            raise
        except sqlalchemy.exc.DatabaseError as e:
            self.logger.error("Can't connecto to MySQL database", exc_info=e)
            raise

    def exec_sql_write(self, statement):
        try:
            with self.mysql_engine.connect() as connection:
                connection.execute(statement)
        except Exception as e:
            self.logger.error(
                "Error occured during executing query", query=statement, exc_info=e
            )
            raise

    def exec_sql_dataframe_write(self, dataframe, table, **kwargs):
        dataframe.to_sql(table, self.mysql_engine, **kwargs)

    def check_mysql_available(self):
        """Check if a basic mysql query gives a valid response"""
        query = "SHOW DATABASES"
        response = self.exec_sql_query(query)

        available = len(list(response["Database"])) > 0

        return available
