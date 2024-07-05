# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
from openstef_dbc.services.weather import Weather


@patch("openstef_dbc.data_interface._DataInterface.get_instance")
class TestGetNearestWeatherLocations(unittest.TestCase):
    def test(self, data_interface_get_instance_mock):
        # Mock other functions
        weather = Weather()
        cities = [
            {"city": "A", "lat": 42.3, "lon": 2.2, "country": "FR"},
            {"city": "B", "lat": 43.3, "lon": 2.6, "country": "FR"},
            {"city": "C", "lat": 42.5, "lon": 2.6, "country": "FR"},
            {"city": "D", "lat": 42.3, "lon": 2.6, "country": "FR"},
            {"city": "E", "lat": 42.3, "lon": 2.8, "country": "FR"},
        ]
        weather.get_weather_forecast_locations = lambda country, active: cities

        # Test one location with equal distance (D and E)
        df_check = weather._get_nearest_weather_locations(
            location=[42.3, 2.7], country="FR", number_locations=1
        )
        # Check for correct results
        expected_response = pd.Series("D", index=[8.2])
        expected_response.name = "input_city"
        self.assertTrue(df_check.equals(expected_response))

        # Test two locations
        df_check = weather._get_nearest_weather_locations(
            location=[42.3, 2.7], country="FR", number_locations=2
        )
        # Check for correct results
        expected_response = pd.Series(["D", "E"], index=[8.2, 8.2])
        expected_response.name = "input_city"

        self.assertTrue(df_check.equals(expected_response))


if __name__ == "__main__":
    unittest.main()
