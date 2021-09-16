# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from unittest.mock import MagicMock, patch

from openstf_dbc.database import DataBase


@patch("openstf_dbc.database._DataInterface", MagicMock())
class TestDatabase(unittest.TestCase):
    def test_init(self):
        config = MagicMock()
        DataBase(config)


if __name__ == "__main__":
    unittest.main()
