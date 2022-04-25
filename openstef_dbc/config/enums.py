# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from enum import Enum


class RuntimeEnv(Enum):
    LOCAL = "local"
    CONTAINER = "container"


class Namespace(Enum):
    ACC = "icarus-acc"
    PRD = "icarus-prd"
