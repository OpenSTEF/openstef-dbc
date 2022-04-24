# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
import warnings
from unittest.mock import MagicMock, patch

from openstef_dbc.database import DataBase
from tests.data.config import config


@patch("openstef_dbc.database._DataInterface", MagicMock())
class TestDatabase(unittest.TestCase):
    def test_init(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            DataBase(config)


if __name__ == "__main__":
    unittest.main()
