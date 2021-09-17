# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
from openstf_dbc.services.predictor import Predictor


class TestPredictor(unittest.TestCase):
    @patch("openstf_dbc.services.predictor._DataInterface")
    @patch("openstf_dbc.services.predictor.Weather")
    def test_get_predictors(self, weather_service_mock, data_interface_mock):
        # TODO this is not straighforware you would have to mock all exec_sql_query
        # calls or allow everything to be an empty dataframe
        pass
