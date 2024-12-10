# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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

combined_weatherdata_with_tAhead = pd.read_csv(
    DATA_FOLDER / "combined_weatherdata_with_tAhead_test_data.csv",
    sep=";",
    index_col=0,
    parse_dates=["_time"],
)

locations = [
    {"city": "Rotterdam", "lat": 51.926517, "lon": 4.462456, "country": "NL"},
    {"city": "Amsterdam", "lat": 52.377956, "lon": 4.897070, "country": "NL"},
]

weather = Weather()

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

    @patch("openstef_dbc.services.weather._DataInterface")
    def test_get_single_location_weather_data_with_tAhead(self, MockDataInterface):
        """Data: dataframe contains weather data of multiple
        locations for same timpestamp with np.nan-values

        Expected: return same dataframe
        """

        datetime_start = pd.to_datetime("2022-01-01 00:00:00+00:00")
        datetime_end = pd.to_datetime("2022-01-01 02:00:00+00:00")

        mock_Datainterface = MagicMock()
        MockDataInterface.get_instance.return_value = mock_Datainterface
        mock_Datainterface.exec_influx_query.return_value = (
            combined_weatherdata_with_tAhead
        )

        weather = Weather()
        # Mocking influx query and get_weather_forecast_locations
        weather.get_weather_forecast_locations = lambda country, active: locations

        response = weather.get_weather_tAhead_data(
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
                "created": [
                    pd.Timestamp("2022-01-01 00:00:00+0000", tz="UTC"),
                    pd.Timestamp("2022-01-01 00:00:00+0000", tz="UTC"),
                    pd.Timestamp("2022-01-01 00:00:00+0000", tz="UTC"),
                    pd.Timestamp("2022-01-01 00:00:00+0000", tz="UTC"),
                    pd.Timestamp("2022-01-01 00:00:00+0000", tz="UTC"),
                    pd.Timestamp("2022-01-01 01:00:00+0000", tz="UTC"),
                    pd.Timestamp("2022-01-01 01:00:00+0000", tz="UTC"),
                    pd.Timestamp("2022-01-01 01:00:00+0000", tz="UTC"),
                ],
                "source": [
                    "harm_arome",
                    np.nan,
                    "harm_arome",
                    np.nan,
                    "harm_arome",
                    "harm_arome",
                    np.nan,
                    "harm_arome",
                ],
                "tAhead": [0.0, 0.5, 1.0, 1.5, 2.0, 0.0, 0.5, 1.0],
                "windspeed": [3.6, 3.6, 3.6, 2.5, 1.5, 0.6, 1.4, 2.1],
            },
            index=[
                pd.Timestamp("2022-01-01 00:00:00+0000", tz="UTC"),
                pd.Timestamp("2022-01-01 00:30:00+0000", tz="UTC"),
                pd.Timestamp("2022-01-01 01:00:00+0000", tz="UTC"),
                pd.Timestamp("2022-01-01 01:30:00+0000", tz="UTC"),
                pd.Timestamp("2022-01-01 02:00:00+0000", tz="UTC"),
                pd.Timestamp("2022-01-01 01:00:00+0000", tz="UTC"),
                pd.Timestamp("2022-01-01 01:30:00+0000", tz="UTC"),
                pd.Timestamp("2022-01-01 02:00:00+0000", tz="UTC"),
            ],
        )
        expected_response.index.name = "datetime"

        self.assertTrue(response.equals(expected_response))
        
    def test_source_run_valid_input(self):
        """Test with valid input."""
        datetime_input = pd.Series(datetime(2024, 12, 10, 15, 30))  # December 10 2024 at 15:30 
        tAhead = pd.Series(6)
        expected_result = pd.Series(datetime(2024, 12, 10, 9, 30))  # 6 hours ahead
        pd.testing.assert_series_equal(weather._get_source_run(datetime_input, tAhead), expected_result)

    def test_source_run_vzero_tAhead(self):
        """Test with tAhead equal to 0 (no substrasction)."""
        datetime_input = pd.Series(datetime(2024, 12, 10, 15, 30))
        tAhead = pd.Series(0)
        pd.testing.assert_series_equal(weather._get_source_run(datetime_input, tAhead), datetime_input)

    def test_source_run_negative_tAhead(self):
        """Test with negative tAhead """
        datetime_input = pd.Series(datetime(2024, 12, 10, 15, 30))
        tAhead = pd.Series(-3)
        expected_result = pd.Series(datetime(2024, 12, 10, 18, 30))  # 3 heures after
        pd.testing.assert_series_equal(weather._get_source_run(datetime_input, tAhead), expected_result)

    def test_source_run_large_tAhead(self):
        """Test with very large tAhead """
        datetime_input = pd.Series(datetime(2024, 12, 10, 15, 30))
        tAhead = pd.Series(1000)
        expected_result = pd.Series(datetime(2024, 10, 29, 23, 30))  # 1000 heures ahead
        pd.testing.assert_series_equal(weather._get_source_run(datetime_input, tAhead), expected_result)

    def test_source_run_invalid_datetime_input(self):
        """Test with unvalid datetime."""
        datetime_input = pd.Series("2024-12-10 15:30")  # Not a datetime object
        tAhead = pd.Series(6)
        with self.assertRaises(ValueError):
            weather._get_source_run(datetime_input, tAhead)

    def test_source_run_invalid_tAhead(self):
        """Test with unvalid tAhead """
        datetime_input = pd.Series(datetime(2024, 12, 10, 15, 30))
        tAhead = pd.Series("six")  # Not a number
        with self.assertRaises(ValueError):
            weather._get_source_run(datetime_input, tAhead)

    def test_source_run_float_tAhead(self):
        """Test with a float tAhead."""
        datetime_input = pd.Series(datetime(2024, 12, 10, 15, 30))
        tAhead = pd.Series(2.5)
        expected_result = pd.Series(datetime(2024, 12, 10, 13, 0))  # 2 hours and 30 minutes ahead
        pd.testing.assert_series_equal(weather._get_source_run(datetime_input, tAhead), expected_result)
    
    def test_source_run_int_tAhead(self):
        """Test with a int tAhead."""
        datetime_input = pd.Series(datetime(2024, 12, 10, 15, 30))
        tAhead = pd.Series(2)
        expected_result = pd.Series(datetime(2024, 12, 10, 13, 30))  # 2 hours
        pd.testing.assert_series_equal(weather._get_source_run(datetime_input, tAhead), expected_result)

if __name__ == "__main__":
    unittest.main()
