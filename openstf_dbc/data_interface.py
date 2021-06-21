# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import geopy
import influxdb
import pandas as pd
import requests
import sqlalchemy

from openstf_dbc.config.config import ConfigManager
from openstf_dbc.ktp_api import KtpApi
from openstf_dbc.log import logging


# Define abstract interface


class _DataInterface:

    _instance = None

    def __init__(self):
        """All connections and queries to the influx and mysql databases
        are governed by this class"""

        # Check if we already have an instance
        if self._instance is not None:
            raise RuntimeError("This is a singleton class, can only init once")
        self._instance = self

        self.config = ConfigManager.get_instance()
        self.logger = logging.get_logger(self.__class__.__name__)
        self.ktp_api = KtpApi()

        # Set geopy proxies
        # https://geopy.readthedocs.io/en/stable/#geopy.geocoders.options
        # https://docs.python.org/3/library/urllib.request.html#urllib.request.ProxyHandler
        # By default the system proxies are respected
        # (e.g. HTTP_PROXY and HTTPS_PROXY env vars or platform-specific proxy settings,
        # such as macOS or Windows native preferences – see
        # urllib.request.ProxyHandler for more details).
        # The proxies value for using system proxies is None.
        geopy.geocoders.options.default_proxies = self.config.proxies

        self._connect_to_database(use_influxdb=True, use_mysql=True)

    @staticmethod
    def get_instance():
        if _DataInterface._instance is None:
            _DataInterface._instance = _DataInterface()
        return _DataInterface._instance

    def _connect_to_database(self, use_influxdb=True, use_mysql=True):
        # Create Influx client
        if use_influxdb is True:
            self.influx_client = self._create_influx_client()

        # Create SQL engine
        if use_mysql is True:
            self.sql_engine = self._create_sql_write_engine()

    def _create_influx_client(self):
        """Create influx client, namespace-dependend"""
        try:
            return influxdb.DataFrameClient(
                host=self.config.influxdb.host,
                port=self.config.influxdb.port,
                username=self.config.influxdb.username,
                password=self.config.influxdb.password,
            )
        except Exception as e:
            self.logger("Could not connect to InfluxDB database", exc_info=e)
            raise

    def _create_sql_write_engine(self):
        """Create sql_write_engine, namespace-dependend.
        Differs from sql_connection in the sense that this write_engine
        *can* write pandas dataframe directly"""
        database_url = (
            "mysql+mysqlconnector://{user}:{pw}@{host}:{port}/{db_name}?use_pure=True"
        ).format(
            user=self.config.mysql.username,
            pw=self.config.mysql.password,
            host=self.config.mysql.host,
            port=self.config.mysql.port,
            db_name=self.config.mysql.database_name,
        )
        try:
            return sqlalchemy.create_engine(database_url)
        except Exception as e:
            self.logger.error("Could not connect to MySQL database", exc_info=e)
            raise

    def exec_influx_query(self, query):
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
            return pd.read_sql(query, self.sql_engine, **kwargs)
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
            with self.sql_engine.connect() as connection:
                connection.execute(statement)
        except Exception as e:
            self.logger.error(
                "Error occured during executing query", query=statement, exc_info=e
            )
            raise

    def exec_sql_dataframe_write(self, dataframe, table, **kwargs):
        dataframe.to_sql(table, self.sql_engine, **kwargs)

    def check_mysql_available(self):
        """Check if a basic mysql query gives a valid response"""
        query = "SHOW DATABASES"
        response = self.exec_sql_query(query)

        available = len(list(response["Database"])) > 0

        return available
