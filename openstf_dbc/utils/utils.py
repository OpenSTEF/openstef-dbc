# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
import pandas as pd


def get_datetime_index(start, end, freq):
    return pd.date_range(
        start=start,
        end=end,
        freq=freq,
        tz="UTC",
    )
