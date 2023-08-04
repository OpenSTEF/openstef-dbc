# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
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


def parse_influx_result(
    result: pd.DataFrame, aditional_indices: list[str] = None, aggfunc="mean"
) -> pd.DataFrame:
    """Parse resulting DataFrame of flux query to a format we expect in the rest of the lib."""
    indices = ["_time"]
    if aditional_indices is not None:
        indices.extend(aditional_indices)

    result["_time"] = pd.to_datetime(result["_time"])
    result = result.pivot_table(
        columns="_field", values="_value", index=indices, aggfunc=aggfunc
    )
    result = result.reset_index().set_index("_time")
    result.index.name = "datetime"
    result.columns.name = ""
    return result


def round_time_difference(time_diff, options):
    """Rounds a time difference to the first bigger option.

    Used to calculate the tAhead for forecasts"""
    next_bigger_option = next((opt for opt in options if opt > time_diff), None)
    if next_bigger_option is None:
        return time_diff
    return next_bigger_option


max_timediff = 1000


def round_single_time_diff(time_diff, options):
    """Rounds a time difference to the first bigger option.
    Used to calculate the tAhead for forecasts"""

    next_bigger_option = next((opt for opt in options if opt > time_diff), None)
    if next_bigger_option is None:
        return max_timediff
    return next_bigger_option


def round_time_differences(time_diffs, options):
    """Round a number of time diffs to the first bigger option"""
    rounded_times = [
        round_single_time_diff(time_diff, options) for time_diff in time_diffs
    ]
    return rounded_times
