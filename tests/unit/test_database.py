import unittest
from unittest.mock import patch, MagicMock

from openstf_dbc.database import DataBase


@patch("openstf_dbc.database._DataInterface", MagicMock())
class TestDatabase(unittest.TestCase):

    def test_init(self):
        DataBase()


if __name__ == "__main__":
    unittest.main()
