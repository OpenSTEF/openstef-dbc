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
        
    def test_sql_db_available(self):
        
        assert self.di.check_sql_available() == True       
        
        
    def test_get_prediction_jobs(self):
        
        pj_retriever = PredictionJobRetriever()
        
        response = pj_retriever.get_prediction_jobs()       
        assert isinstance(response, list)
        assert isinstance(response[0], PredictionJobDataClass)

        
        
    def test_get_pids_for_api_key(self):
        
        pj_retriever = PredictionJobRetriever()

        response = pj_retriever.get_pids_for_api_key('random_api_key')
        assert isinstance(response, list)

    
    # def test_get_ean_for_pid(self):
    
    # def test_add_quantiles_to_prediction_jobs(self):
    
    # def test_get_systems_near_location(self):
    
    # def test_get_systems_by_pid(self):
    
    # def test_get_random_pv_systems(self):
    
    # def test_get_api_key_for_system(self):
    
    # def test_write_location(self):
    
    # def test_get_power_curve(self):
    
    # def test_get_energy_split_coefs(self):
    
    # def test_get_weather_forecast_locations(self):
    
    # def test_get_coordinates_of_location(self):
        