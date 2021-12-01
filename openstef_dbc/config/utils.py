# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import os
from pathlib import Path

from openstef_dbc.config.enums import RuntimeEnv, Namespace


def determine_runtime_environment():
    """Determine the runtime environment

    Returns:
        RunTimeEnv: Runtime environment (LOCAL or CONTAINER)
    """
    if "KUBERNETES_SERVICE_HOST" in os.environ:
        return RuntimeEnv.CONTAINER
    return RuntimeEnv.LOCAL


def determine_local_namespace():
    """Determine the local development namespace.

    During development the namespace to use will be writtern from the `namespace.txt`
    file. This content of this file is controlled by the external `icarus` script.
    The development namespace may also be overwritten by a the KTP_NAMESPACE
    environmental variable

    Returns:
        str: Development namespace
    """

    # Set default to acc
    namespace = Namespace.ACC

    for path in os.environ.get("PATH").split(os.pathsep):
        path = Path(path)
        # Two possible locations, depening on your path setup
        locations = [path / "icarus-scripts" / "namespace.txt", path / "namespace.txt"]
        for location in locations:
            if location.is_file() is False:
                continue
            with open(location, "rt") as f:
                if "prd" in f.read().strip().lower():
                    namespace = Namespace.PRD
            return namespace

    return namespace


def merge(source, overrides):
    """Merge a nested dictionary in place."""
    # initialize empty dict when source is None
    if source is None:
        source = {}

    for key, value in overrides.items():
        if isinstance(value, dict) and value:
            returned = merge(source.get(key, {}), value)
            source[key] = returned
            continue
        source[key] = overrides[key]
    return source
