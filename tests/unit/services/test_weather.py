# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from openstf_dbc.services.weather import Weather
from tests.utils.base import BaseTestCase

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

noncombined_weatherdata_nomissing = pd.read_csv(
    DATA_FOLDER / "noncombined_weatherdata_nomissing_test_data.csv",
    sep=";",
    index_col=0,
    parse_dates=["datetime"],
)

combined_weatherdata_nomissing = pd.read_csv(
    DATA_FOLDER / "combined_weatherdata_nomissing_test_data.csv",
    sep=";",
    index_col=0,
    parse_dates=["datetime"],
)

combined_weatherdata_DSN = pd.read_csv(
    DATA_FOLDER / "combined_weatherdata_DSN_test_data.csv",
    sep=";",
    index_col=0,
    parse_dates=["datetime"],
)


@patch("openstf_dbc.services.weather._DataInterface", MagicMock())
@patch("openstf_dbc.services.weather.Write", MagicMock())
class TestWeather(BaseTestCase):
    def test_combine_weather_sources_fill_nan_values(self):
        """Data: dataframe contains weather data of multiple sources for same timpestamp with nan-values

        Expected: dataframe without duplicate timestamps containing data from multiple data sources without nan-values
        """
        database = Weather()
        response = database._combine_weather_sources(result=noncombined_weatherdata)
        expected_response = combined_weatherdata
        self.assertDataframeEqual(expected_response, response)

    def test_no_nan_values(self):
        """Data: dataframe contains weather data of multiple sources for same timpestamp without nan-values

        Expected: dataframe containing data only from preferred data source. No duplicate timestamps
        """
        database = Weather()
        response = database._combine_weather_sources(
            result=noncombined_weatherdata_nomissing
        )
        expected_response = combined_weatherdata_nomissing
        self.assertDataframeEqual(expected_response, response)

    @unittest.skip(
        "TO FIX: assert fails pressure column is of type object (str) in response"
    )
    def test_different_source_order(self):
        """Data: dataframe contains weather data of multiple sources for same timpestamp with nan-values

        Expected: dataframe without duplicate timestamps containing data from mostly DSN data source without nan-values
        """
        database = Weather()
        response = database._combine_weather_sources(
            result=noncombined_weatherdata,
            source_order=["DSN", "harmonie", "harm_arome"],
        )
        expected_response = combined_weatherdata_DSN
        self.assertDataframeEqual(expected_response, response)

    def test_non_optimum_source(self):
        """Data: dataframe contains weather data of one source

        Expected: return same dataframe
        """
        database = Weather()
        response = database._combine_weather_sources(result=combined_weatherdata)
        expected_response = combined_weatherdata
        self.assertDataframeEqual(expected_response, response)


if __name__ == "__main__":
    unittest.main()
