# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from dataclasses import dataclass
from datetime import datetime

DATETIME_FMT = "%Y-%m-%dT%H:%M:%S%z"


@dataclass
class SwitchState:
    datetime: str
    sid: str
    normal_open: bool
    currently_open: bool

    def __post_init__(self):
        # try to convert str back to datatime to check format
        datetime.strptime(self.datetime, DATETIME_FMT)
