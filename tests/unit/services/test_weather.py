# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
from openstef_dbc.services.weather import Weather
from openstef_dbc.data_interface import _DataInterface
from tests.unit.utils.base import BaseTestCase

DATA_FOLDER = Path(__file__).absolute().parent.parent / "data"

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

multiple_location_weatherdata = pd.read_csv(
    DATA_FOLDER / "multiple_locations_weatherdata_nomissing_test_data.csv",
    sep=";",
    index_col=0,
    parse_dates=["_time"],
)

locations = [
    {"city": "Rotterdam", "lat": 51.926517, "lon": 4.462456, "country": "NL"},
    {"city": "Amsterdam", "lat": 52.377956, "lon": 4.897070, "country": "NL"},
]


@patch("openstef_dbc.services.weather.Write", MagicMock())
class TestWeather(BaseTestCase):
    @patch("openstef_dbc.services.weather._DataInterface", MagicMock())
    def test_combine_weather_sources_fill_nan_values(self):
        """Data: dataframe contains weather data of multiple sources for same timpestamp with np.nan-values

        Expected: dataframe without duplicate timestamps containing data from multiple data sources without np.nan-values
        """
        database = Weather()
        response = database._combine_weather_sources(result=noncombined_weatherdata)
        expected_response = combined_weatherdata
        self.assertDataframeEqual(expected_response, response)

    def test_no_nan_values(self):
        """Data: dataframe contains weather data of multiple sources for same timpestamp without np.nan-values

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
        """Data: dataframe contains weather data of multiple sources for same timpestamp with np.nan-values

        Expected: dataframe without duplicate timestamps containing data from mostly DSN data source without np.nan-values
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

    @patch("openstef_dbc.services.weather._DataInterface")
    def test_get_multiple_location_weather_data(self, MockDataInterface):
        """Data: dataframe contains weather data of multiple
        locations for same timpestamp with np.nan-values

        Expected: return same dataframe
        """

        datetime_start = pd.to_datetime("2022-01-01 00:00:00+00:00")
        datetime_end = pd.to_datetime("2022-01-01 02:00:00+00:00")

        mock_instance = MagicMock()
        MockDataInterface.get_instance.return_value = mock_instance

        weather = Weather()
        # Mocking influx query et get_weather_forecast_locations
        weather.get_weather_forecast_locations = lambda country, active: locations
        nearest_location = weather._get_nearest_weather_locations(
            location=[52, 4.7], number_locations=2
        ).to_list()

        query = f"input_city in {nearest_location} & _time > '{datetime_start}+00:00' & _time <= '{datetime_end}+00:00' & _field == 'windspeed'"
        mock_instance.exec_influx_query.return_value = (
            multiple_location_weatherdata.query(query)
        )

        response = weather.get_weather_data(
            location=[52, 4.7],
            weatherparams="windspeed",
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            number_locations=2,
            resolution="30min",
        )
        response.windspeed = np.round(response.windspeed, 1)

        expected_response = pd.DataFrame(
            {
                "input_city": [
                    "Amsterdam",
                    "Amsterdam",
                    "Amsterdam",
                    "Rotterdam",
                    "Rotterdam",
                    "Rotterdam",
                ],
                "source": [
                    "harm_arome",
                    np.nan,
                    "harm_arome",
                    "harm_arome",
                    np.nan,
                    "harm_arome",
                ],
                "windspeed": [6.9, 4.0, 1.2, 7.6, 4.5, 1.5],
                "distance": [44.2, 44.2, 44.2, 18.3, 18.3, 18.3],
            },
            index=np.tile(
                pd.date_range(
                    start="2022-01-01 01:00:00+00:00",
                    end="2022-01-01 02:00:00+00:00",
                    freq="30min",
                ),
                2,
            ),
        )
        expected_response.index.name = "datetime"

        self.assertTrue(response.equals(expected_response))

    @patch("openstef_dbc.services.weather._DataInterface")
    def test_get_single_location_weather_data(self, MockDataInterface):
        """Data: dataframe contains weather data of multiple
        locations for same timpestamp with np.nan-values

        Expected: return same dataframe
        """

        datetime_start = pd.to_datetime("2022-01-01 00:00:00+00:00")
        datetime_end = pd.to_datetime("2022-01-01 02:00:00+00:00")

        mock_instance = MagicMock()
        MockDataInterface.get_instance.return_value = mock_instance

        weather = Weather()
        # Mocking influx query and get_weather_forecast_locations
        weather.get_weather_forecast_locations = lambda country, active: locations

        nearest_location = weather._get_nearest_weather_locations(
            location=[52, 4.7], number_locations=1
        ).to_list()

        query = f"input_city in {nearest_location} & _time > '{datetime_start}+00:00' & _time <= '{datetime_end}+00:00' & _field == 'windspeed'"
        mock_instance.exec_influx_query.return_value = (
            multiple_location_weatherdata.query(query)
        )

        response = weather.get_weather_data(
            location=[52, 4.7],
            weatherparams="windspeed",
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            number_locations=1,
            resolution="30min",
        )
        response.windspeed = np.round(response.windspeed, 1)

        expected_response = pd.DataFrame(
            {
                "source": ["harm_arome", np.nan, "harm_arome"],
                "windspeed": [7.6, 4.5, 1.5],
            },
            index=pd.date_range(
                start="2022-01-01 01:00:00+00:00",
                end="2022-01-01 02:00:00+00:00",
                freq="30min",
            ),
        )
        expected_response.index.name = "datetime"

        self.assertTrue(response.equals(expected_response))


if __name__ == "__main__":
    unittest.main()
