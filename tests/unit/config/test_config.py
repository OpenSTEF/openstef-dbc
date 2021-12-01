# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

# import builtins
import unittest
from unittest.mock import patch, MagicMock

# import project modules
from openstf_dbc.config.config import ConfigManager
from openstf_dbc.config.enums import RuntimeEnv


@patch("openstf_dbc.config.config.logging", MagicMock())
@patch("openstf_dbc.config.config.ConfigBuilder", MagicMock())
@patch("openstf_dbc.config.utils.determine_runtime_environment")
class TestConfigManager(unittest.TestCase):
    def setUp(self):
        # reset singleton before every tests
        ConfigManager._loaded_config = None
        ConfigManager._instance = None

    def test_get_instance_local(self, runtime_env_mock):
        runtime_env_mock.return_value = RuntimeEnv.LOCAL
        config = ConfigManager.get_instance()
        self.assertEqual(type(config), ConfigManager)

    def test_get_instance_container_unhappy_flow(self, runtime_env_mock):
        runtime_env_mock.return_value = RuntimeEnv.CONTAINER
        # unhappy flow (project config not loaded)
        self.assertRaises(RuntimeError, ConfigManager.get_instance)

    def test_get_instance_container_happy_flow(self, runtime_env_mock):
        runtime_env_mock.return_value = RuntimeEnv.CONTAINER
        # happy flow ()
        ConfigManager.load_project_config("project_root")
        config = ConfigManager.get_instance()
        self.assertEqual(type(config), ConfigManager)


if __name__ == "__main__":
    unittest.main()
