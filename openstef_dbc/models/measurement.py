# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from dataclasses import dataclass, asdict
from typing import List
from datetime import datetime

DATETIME_FMT = "%Y-%m-%dT%H:%M:%S%z"


@dataclass
class MeasurementData:
    datetime: str
    output: float

    def __post_init__(self):
        # try to convert str back to datatime to check format
        datetime.strptime(self.datetime, DATETIME_FMT)


@dataclass
class Measurement:
    data: List[MeasurementData]
    sid: str

    def to_dict(self):
        return asdict(self)
