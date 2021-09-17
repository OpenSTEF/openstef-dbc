# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
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
