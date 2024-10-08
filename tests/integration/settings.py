# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from typing import Union

from pydantic_settings import BaseSettings


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
    sql_db_username: str = "test"
    sql_db_password: str = "test"
    sql_db_host: str = "localhost"
    sql_db_port: int = 1234
    sql_db_database_name: str = "test"
    proxies: Union[dict[str, str], None] = None
