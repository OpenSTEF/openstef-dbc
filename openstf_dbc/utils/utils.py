import pandas as pd


def get_datetime_index(start, end, freq):
    return pd.date_range(
        start=start,
        end=end,
        freq=freq,
        tz="UTC",
    )
