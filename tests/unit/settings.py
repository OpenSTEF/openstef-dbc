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
    influx_organization: str = "myorg"
    influxdb_token: str = "token"
    influxdb_host: str = "host"
    influxdb_port: str = "123"
    mysql_username: str = "test"
    mysql_password: str = "test"
    mysql_host: str = "host"
    mysql_port: int = 123
    mysql_database_name: str = "database_name"
    proxies: Union[dict[str, str], None] = None
