# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import pandas as pd
from pandas.testing import assert_frame_equal

# from openstf_dbc.database import DataBase


time = datetime.utcnow()

DATA_FOLDER = Path(__file__).absolute().parent.parent.parent / "data"

noncombined_weatherdata = pd.read_csv(
    DATA_FOLDER / "noncombined_weatherdata_test_data.csv",
    sep=";",
    index_col=0,
    parse_dates=["datetime"],
)

combined_weatherdata = pd.read_csv(
    DATA_FOLDER / "combined_weatherdata_test_data.csv",
    sep=";",
    index_col=0,
    parse_dates=["datetime"],
)

weatherdata = pd.DataFrame(
    index=pd.to_datetime(["2019-01-01 10:00:00", "2019-01-01 10:15:00"]),
    data=dict(clouds=[15, 20], ratiation=[10, 5]),
)


@mock.patch(
    "openstf_dbc.database.DataBase", **{"get_weatherdata.return_value": weatherdata}
)
def test_mock_query(db_mock):
    # Variables
    loc = "Volkel"
    weatherparams = (["clouds"], ["clouds", "temp"])
    sources = (["harmonie"], ["harmonie", "harm_arome"])
    start = datetime.utcnow() - timedelta(days=2)
    end = datetime.utcnow()

    # Run function
    expected_response = pd.DataFrame(
        index=pd.to_datetime(["2019-01-01 10:00:00", "2019-01-01 10:15:00"]),
        data=dict(clouds=[15, 20], ratiation=[10, 5]),
    )
    # expected_response.index.name = "datetime"

    for weather_param in weatherparams:
        for source in sources:
            response = db_mock.get_weatherdata(loc, weather_param, start, end, source)
            pd.testing.assert_frame_equal(response, expected_response)


@mock.patch(
    "openstf_dbc.database.DataBase",
    **{"get_weatherdata.return_value": combined_weatherdata}
)
def test_combine_data_optimum(db_mock):
    """Data: dataframe contains weather data of multiple sources for same timpestamp with nan-values

    Expected: dataframe without duplicate timestamps containing data from multiple data sources without nan-values
    """
    location = "De Bilt"
    weather_params = [
        "clouds",
        "temp",
        "rain",
        "winddeg",
        "windspeed",
        "pressure",
        "humidity",
        "radiation",
    ]

    response = db_mock.get_weatherdata(
        location_name=location, weatherparams=weather_params
    )
    expected_response = combined_weatherdata
    assert_frame_equal(expected_response, response)


# Run all tests
if __name__ == "__main__":
    tests = [eval(item) for item in dir() if item[0 : len("test_")] == "test_"]
    for test in tests:
        print("executing:", test)
        test()
    print("Succes!")
