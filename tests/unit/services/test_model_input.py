# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from unittest.mock import patch

import pandas as pd
from openstef_dbc.services.model_input import ModelInput


class TestModelInput(unittest.TestCase):
    @patch("openstef_dbc.services.model_input.Predictor")
    @patch("openstef_dbc.services.model_input.Ems")
    def test_model_input_empty_data(self, ems_service_mock, predictor_service_mock):
        ems_service_mock.return_value.get_load_pid.return_value = pd.DataFrame()
        predictor_service_mock.return_value.get_predictors.return_value = pd.DataFrame()
        ModelInput().get_model_input()

    @patch("openstef_dbc.services.model_input.Predictor")
    @patch("openstef_dbc.services.model_input.Ems")
    def test_model_input_with_weather_sources(
        self, ems_service_mock, predictor_service_mock
    ):
        # Arrange
        weather_sources = ["weather_source_1", "weather_source_2"]
        ems_service_mock.return_value.get_load_pid.return_value = pd.DataFrame()
        predictor_service_mock.return_value.get_predictors.return_value = pd.DataFrame()

        # Act
        ModelInput().get_model_input(weather_source=weather_sources)

        # Assert
        self.assertEqual(
            predictor_service_mock.return_value.get_predictors.call_args[1]["source"],
            weather_sources,
        )
