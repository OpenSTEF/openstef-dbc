# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from types import SimpleNamespace

import openstf_dbc.config.utils as utils
from openstf_dbc.config.builder import ConfigBuilder
from openstf_dbc.config.enums import RuntimeEnv
from openstf_dbc.log import logging


class ConfigManager:

    logger = logging.get_logger("ConfigManager")

    _instance = None
    _loaded_config = None

    class ConfigGroup(SimpleNamespace):
        pass

    def __init__(self, config):
        if self._instance is not None:
            raise RuntimeError("This is a singleton class, can only init once")

        self.env = utils.determine_runtime_environment()

        # store config and secrets on this instance
        self._add_config_properties(config)

        self._instance = self

    @staticmethod
    def load_project_config(project_root):
        ConfigManager._loaded_config = ConfigBuilder.build_project_config(project_root)
        return ConfigManager

    @staticmethod
    def get_instance():
        # return singleton if already initialized
        if ConfigManager._instance is not None:
            return ConfigManager._instance
        # initialize if project config is already loaded
        if ConfigManager._loaded_config is not None:
            return ConfigManager(ConfigManager._loaded_config)
        # build default if no config loaded and running locally (local use of openstf_dbc)
        if utils.determine_runtime_environment() is RuntimeEnv.LOCAL:
            ConfigManager.logger.warning(
                "No project configuration loaded, using default for local development"
            )
            return ConfigManager(ConfigBuilder.build_default_config())

        raise RuntimeError("No configuration loaded")

    def _add_config_properties(self, config):
        for key, value in config.items():
            # create single property
            if type(value) is not dict:
                setattr(self, key, value)
                continue
            # proxies need to be stored as a dict
            if key == "proxies":
                setattr(self, key, value)
                continue
            # create config group
            group = self.ConfigGroup()
            # fill the group with dict key, values
            for k, v in value.items():
                setattr(group, k, v)
            # add the group
            setattr(self, key, group)
