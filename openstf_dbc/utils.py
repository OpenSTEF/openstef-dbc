# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
import datetime

import pandas as pd

DEFAULT_FREQ = "15min"
DEFAULT_TZ = datetime.timezone.utc


def genereate_datetime_index(start, end, freq=None):
    # Use timezone info from start if given
    if start.tzinfo is None:
        tz = DEFAULT_TZ
    else:
        tz = start.tzinfo

    if not freq:
        freq = DEFAULT_FREQ

    datetime_index = pd.date_range(
        start=start,
        end=end,
        freq=freq,
        tz=tz,
    )
    # Round to given frequency
    datetime_index = datetime_index.round(freq)

    return datetime_index


def process_datetime_range(start, end, freq=None):
    datetime_index = genereate_datetime_index(start, end, freq)

    # Use the index to generate valid start and end times
    datetime_start = datetime_index[0].to_pydatetime()
    datetime_end = datetime_index[-1].to_pydatetime()

    return datetime_start, datetime_end, datetime_index
