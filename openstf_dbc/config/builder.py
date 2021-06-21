# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import os
from pathlib import Path

import yaml

import openstf_dbc.config.utils as utils
from openstf_dbc.config.enums import RuntimeEnv
from openstf_dbc.log import logging

LOCAL_KTP_CONFIG_DIR = Path.home() / "ktp"

# Main config files (required, git)
MAIN_CONFIG_REL_PATH = "k8s/{namespace}/config/config.yaml"
# local config files (optional, git)
LOCAL_CONFIG_REL_PATH = "k8s/{namespace}/config/config.local.yaml"
# User config file (optional)
USER_CONFIG_REL_PATH = "config.user.yaml"

# Main local config files (when using openstf_dbc locally)
MAIN_LOCAL_CONFIG_PATH = f"{LOCAL_KTP_CONFIG_DIR}/{{namespace}}/config.yaml"

# local secret files
LOCAL_SECRET_PATH = f"{LOCAL_KTP_CONFIG_DIR}/{{namespace}}/secrets.yaml"

DEPLOYED_CLUSTER_CONFIG_PATH = "/config/config.yaml"
SECRETS_ENV_VAR_PREFIX = "KTP_"


class ConfigBuilder:

    logger = logging.get_logger("ConfigBuilder")

    @classmethod
    def build_project_config(cls, project_root):
        env = utils.determine_runtime_environment()

        if env is RuntimeEnv.LOCAL:
            namespace = utils.determine_local_namespace()
            cls.logger.info(
                "Build config for development",
                runtime_env=env.value,
                namespace=namespace.value,
            )
            main_config, local_config, user_config = cls._build_project_config_paths(
                project_root=project_root, namespace=namespace
            )
            config = cls._load_local_config(main_config, local_config, user_config)
            secrets = cls._load_local_secrets(namespace)
            return cls._interpolate_secrets(config, secrets)

        cls.logger.info("Build config for production", runtime_env=env.value)
        config = cls._load_cluster_config()
        secrets = cls._load_cluster_secrets()

        return cls._interpolate_secrets(config, secrets)

    @classmethod
    def build_default_config(cls):
        env = utils.determine_runtime_environment()
        namespace = utils.determine_local_namespace()

        if env is RuntimeEnv.CONTAINER:
            raise ValueError("No default config for production")

        cls.logger.info(
            "Build config for local use",
            runtime_env=env.value,
            namespace=namespace.value,
        )
        main_config = Path(MAIN_LOCAL_CONFIG_PATH.format(namespace=namespace.value))
        config = cls._load_local_config(main_config)
        secrets = cls._load_local_secrets(namespace)
        return cls._interpolate_secrets(config, secrets)

    @classmethod
    def _build_project_config_paths(cls, project_root, namespace):
        main_config = project_root / MAIN_CONFIG_REL_PATH.format(
            namespace=namespace.value
        )
        local_config = project_root / LOCAL_CONFIG_REL_PATH.format(
            namespace=namespace.value
        )
        user_config = project_root / USER_CONFIG_REL_PATH
        return main_config, local_config, user_config

    @classmethod
    def _load_local_config(cls, main_config, local_config=None, user_config=None):
        config_files = [main_config]
        if local_config is not None:
            config_files.append(local_config)

        if user_config is not None:
            config_files.append(user_config)

        config = {}

        for path in config_files:
            # the cluster config is the default and is required
            if path.is_file() is False and path == main_config:
                raise ValueError(f"Main configuration at '{str(path)}' is required")
            # other config is optional
            if path.is_file() is False:
                continue
            cls.logger.info("Loading config file", path=str(path))
            with open(path, "r") as fh:
                loaded_config = yaml.load(fh, Loader=yaml.Loader)
                utils.merge(config, loaded_config)
        return config

    @classmethod
    def _load_local_secrets(cls, namespace):
        secrets_path = Path(LOCAL_SECRET_PATH.format(namespace=namespace.value))

        # return empty dict if no local secrets found
        if secrets_path.is_file() is False:
            cls.logger.warning("No local secrets file found", path=secrets_path)
            secrets = {}
            return secrets

        with open(secrets_path, "r") as fh:
            secrets = yaml.load(fh, Loader=yaml.Loader)
        return secrets

    @classmethod
    def _load_cluster_config(cls):
        cluser_config_path = DEPLOYED_CLUSTER_CONFIG_PATH
        with open(cluser_config_path, "r") as fh:
            config = yaml.load(fh, Loader=yaml.Loader)
        return config

    @classmethod
    def _load_cluster_secrets(
        cls,
    ):
        secrets = {
            k: v for k, v in os.environ.items() if k.startswith(SECRETS_ENV_VAR_PREFIX)
        }
        return secrets

    @classmethod
    def _interpolate_secrets(cls, config, secrets):
        config_with_secrets = {}

        for key, value in config.items():
            # value is in secrets (keys)
            if type(value) is str and value in secrets:
                config_with_secrets[key] = secrets[value]
                continue

            # if anything else except dict, just copy
            if type(value) is not dict:
                config_with_secrets[key] = value
                continue

            # create empty group dict
            config_with_secrets[key] = {}
            # loop over the k, v in the group and look for secret keys
            for k, v in value.items():
                if type(v) is str and v in secrets:
                    config_with_secrets[key][k] = secrets[v]
                    continue
                config_with_secrets[key][k] = v

        return config_with_secrets
