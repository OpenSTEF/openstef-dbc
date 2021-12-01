# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import warnings
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("openstef_dbc")
except PackageNotFoundError:
    # package is not installed
    pass


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        elif cls in cls._instances and len(args) > 0 or len(kwargs) > 0:
            warnings.warn(
                f"Singleton class '{cls.__name__}'' already initialized, arguments are ignored"
            )
        return cls._instances[cls]

    @classmethod
    def get_instance(cls, instance_cls):
        return cls._instances[instance_cls]
