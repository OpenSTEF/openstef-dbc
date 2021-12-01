# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from enum import Enum


class RuntimeEnv(Enum):
    LOCAL = "local"
    CONTAINER = "container"


class Namespace(Enum):
    ACC = "icarus-acc"
    PRD = "icarus-prd"
