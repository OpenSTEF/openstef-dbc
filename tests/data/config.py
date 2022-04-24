# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from unittest import mock

config = mock.MagicMock()
config.mysql.username = "username"
config.mysql.password = "password"
config.mysql.host = "host"
config.mysql.port = 123
config.mysql.database_name = "database_name"
config.influxdb.username = "username"
config.influxdb.password = "password"
config.influxdb.host = "host"
config.influxdb.port = 123
