# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime
import pytz

from pandas import Timestamp
import pandas as pd
import numpy as np
import unittest

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.database import DataBase
from openstef.data_classes.prediction_job import PredictionJobDataClass
from openstef_dbc.services.prediction_job import PredictionJobRetriever
from openstef_dbc.services.systems import Systems
from openstef_dbc.services.model_input import ModelInput
from openstef_dbc.services.splitting import Splitting
from openstef_dbc.services.weather import Weather
from openstef_dbc.services.write import Write
from tests.integration.mock_influx_db_admin import MockInfluxDBAdmin

from tests.integration.settings import Settings

UTC = pytz.timezone("UTC")


class TestDataBaseConnexion(unittest.TestCase):
    def setUp(self) -> None:
        # Initialize settings
        config = Settings()
        self.di = _DataInterface(config)

        # Initialize database object
        self.database = DataBase(config)

    @unittest.skip  # Skip because it need the db to be set up
    def test_sql_db_available(self):
        assert self.di.check_sql_available() == True

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_prediction_jobs(self):
        pj_retriever = PredictionJobRetriever()

        response = pj_retriever.get_prediction_jobs()
        assert isinstance(response, list)
        assert isinstance(response[0], PredictionJobDataClass)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_pids_for_api_key(self):

        pj_retriever = PredictionJobRetriever()

        response = pj_retriever.get_pids_for_api_key("random_api_key")
        assert isinstance(response, list)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_ean_for_pid(self):

        pj_retriever = PredictionJobRetriever()

        response = pj_retriever.get_ean_for_pid(1)
        assert isinstance(response, list)

    @unittest.skip  # Skip because it need the db to be set up
    def test_add_quantiles_to_prediction_jobs(self):

        pj_retriever = PredictionJobRetriever()
        pjs = pj_retriever.get_prediction_jobs()

        response = pj_retriever._add_quantiles_to_prediction_jobs(pjs)[0]
        assert isinstance(response, PredictionJobDataClass)
        assert hasattr(response, "quantiles")

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_systems_near_location(self):

        system = Systems()
        response = system.get_systems_near_location(location=[0.0, 0.0])
        assert isinstance(response, pd.DataFrame)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_systems_by_pid(self):
        system = Systems()
        response = system.get_systems_by_pid(pid=1)
        assert isinstance(response, pd.DataFrame)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_random_pv_systems(self):
        system = Systems()
        response = system.get_random_pv_systems()
        assert isinstance(response, pd.DataFrame)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_api_key_for_system(self):
        system = Systems()
        response = system.get_api_key_for_system(sid="1")
        assert isinstance(response, str)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_power_curve(self):
        modelinput = ModelInput()
        response = modelinput.get_power_curve(turbine_type="Enercon E101")
        assert isinstance(response, dict)
        for key in [
            "name",
            "cut_in",
            "cut_off",
            "kind",
            "manufacturer",
            "peak_capacity",
            "rated_power",
            "slope_center",
            "steepness",
        ]:
            assert key in response

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_energy_split_coefs(self):
        splitting = Splitting()
        pj_retriever = PredictionJobRetriever()
        pj = pj_retriever.get_prediction_jobs()[0]
        response = splitting.get_energy_split_coefs(pj=pj)

        assert isinstance(response, dict)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_weather_forecast_locations(self):
        weather = Weather()
        response = weather.get_weather_forecast_locations()

        assert isinstance(response, list)

    @unittest.skip  # Skip because it need the db to be set up
    def test_get_coordinates_of_location(self):
        weather = Weather()
        response = weather._get_coordinates_of_location(location_name="Leeuwarden")
        assert isinstance(response, tuple)
