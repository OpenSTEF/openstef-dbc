# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch
import pandas as pd
from openstef_dbc.services.prediction_job import (
    PredictionJobRetriever,
    PredictionJobDataClass,
)

prediction_job = {
    "id": 307,
    "name": "Neerijnen",
    "forecast_type": "demand",
    "model": "xgb",
    "model_type_group": "default",
    "horizon_minutes": 2880,
    "resolution_minutes": 15,
    "train_components": 1,
    "lat": 51.8336647,
    "lon": 5.2137814,
    "sid": "LC_Neerijnen",
    "created": pd.Timestamp("2019-04-05 12:08:23"),
}


@patch("openstef_dbc.services.prediction_job._DataInterface")
class TestPredictionJob(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.service = PredictionJobRetriever()

    def test_get_prediction_job_result_size_is_zero(self, data_interface_mock):
        data_interface_mock.get_instance.return_value.exec_sql_query.return_value = (
            pd.DataFrame()
        )

        with self.assertRaises(ValueError):
            self.service.get_prediction_job(pid=307)

    def test_get_prediction_jobs_result_size_is_zero(self, data_interface_mock):

        self.service.get_prediction_jobs()

    def test_get_prediction_jobs_wind_result_size_is_zero(self, data_interface_mock):
        self.service.get_prediction_jobs_wind()

    def test_get_prediction_jobs_solar_result_size_is_zero(self, data_interface_mock):
        self.service.get_prediction_jobs_solar()

    def test_build_get_prediction_jobs_query(self, *args, **kwargs):
        kwargs = {
            "pid": 123,
            "model_type": "xgb",
            "is_active": 1,
            "only_ato": True,
            "limit": 999,
        }
        query = PredictionJobRetriever.build_get_prediction_jobs_query(**kwargs)
        for key, value in kwargs.items():
            if key == "only_ato":
                self.assertTrue("ATO" in query)
                continue
            self.assertTrue(str(value) in query)

    def test_dataclass(self, data_interface_mock):
        pj_dataclass = PredictionJobDataClass(**prediction_job)
        self.assertIsInstance(pj_dataclass, PredictionJobDataClass)

    def test_create_prediction_job_object(self, data_interface_mock):
        pj = self.service._create_prediction_job_object(prediction_job)
        self.assertEqual(pj.__getitem__("id"), prediction_job["id"])
        pj.__setitem__("id", 50)
        self.assertEqual(pj.__getitem__("id"), 50)

        with self.assertRaises(AttributeError):
            pj.__setitem__("non_existing", "can't")

    def test_create_prediction_job_object_missing_attribute(self, data_interface_mock):
        pj_dict = prediction_job.copy()
        pj_dict.pop("forecast_type")
        with self.assertRaises(AttributeError):
            self.service._create_prediction_job_object(pj_dict)


if __name__ == "__main__":
    unittest.main()
