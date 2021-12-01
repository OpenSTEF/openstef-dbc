# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from openstef_dbc import Singleton
from openstef_dbc.database import DataBase
from tests.data.config import config

# Check if DataBase singleton is already initialize
try:
    Singleton.get_instance(DataBase)
# If not initialize
except KeyError:
    DataBase(config)
