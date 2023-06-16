# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from unittest import mock

config = mock.MagicMock()
config.mysql_username = "username"
config.mysql_password = "password"
config.mysql_host = "host"
config.mysql_port = 123
config.mysql_database_name = "database_name"
config.docker_influxdb_init_admin_token = "token"
config.influxdb_host = "host"
config.influxdb_port = 123
