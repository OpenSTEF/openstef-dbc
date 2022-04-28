# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from unittest.mock import patch
import pandas as pd
from openstef.data_classes.prediction_job import PredictionJobDataClass
from openstef_dbc.services.prediction_job import PredictionJobRetriever


class TestPredictionJob(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.service = PredictionJobRetriever()
        self.prediction_job = {
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
    def test_get_prediction_job_result_size_is_zero(self, data_interface_mock):
        data_interface_mock.get_instance.return_value.exec_sql_query.return_value = (
            pd.DataFrame()
        )

        with self.assertRaises(ValueError):
            self.service.get_prediction_job(pid=307)

    @patch("openstef_dbc.services.prediction_job._DataInterface")
    def test_get_prediction_jobs_result_size_is_zero(self, data_interface_mock):

        self.service.get_prediction_jobs()

    @patch("openstef_dbc.services.prediction_job._DataInterface")
    def test_get_prediction_jobs_wind_result_size_is_zero(self, data_interface_mock):
        self.service.get_prediction_jobs_wind()

    @patch("openstef_dbc.services.prediction_job._DataInterface")
    def test_get_prediction_jobs_solar_result_size_is_zero(self, data_interface_mock):
        self.service.get_prediction_jobs_solar()

    @patch("openstef_dbc.services.prediction_job._DataInterface")
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

    @patch("openstef_dbc.services.prediction_job._DataInterface")
    def test_dataclass(self, data_interface_mock):
        pj_dataclass = PredictionJobDataClass(**self.prediction_job)
        self.assertIsInstance(pj_dataclass, PredictionJobDataClass)

    def test_create_prediction_job_object(self):
        pj = self.service._create_prediction_job_object(self.prediction_job)
        self.assertEqual(pj.__getitem__("id"), self.prediction_job["id"])
        pj.__setitem__("id", 50)
        self.assertEqual(pj.__getitem__("id"), 50)

        with self.assertRaises(AttributeError):
            pj.__setitem__("non_existing", "can't")

    def test_create_prediction_job_object_missing_attribute(self):
        pj_dict = self.prediction_job.copy()
        pj_dict.pop("forecast_type")
        with self.assertRaises(AttributeError):
            self.service._create_prediction_job_object(pj_dict)

    def test_create_prediction_job_object_without_train_components(self):
        # Arrange
        pj_dict = self.prediction_job.copy()
        pj_dict.pop("train_components")

        # Act
        pj = self.service._create_prediction_job_object(pj_dict)

        # Assert
        assert isinstance(pj, PredictionJobDataClass)
        assert pj.id == pj_dict["id"]

    @patch(
        "openstef_dbc.services.prediction_job.PredictionJobRetriever._add_description_to_prediction_jobs"
    )
    @patch("openstef_dbc.services.prediction_job._DataInterface.exec_sql_query")
    def test__get_prediction_jobs_query_results(
        self, sql_query_mock, add_description_mock
    ):
        # Arrange
        sql_query_mock.return_value = pd.DataFrame([self.prediction_job])
        add_description_mock.return_value = pd.DataFrame([self.prediction_job]).to_dict(
            "records"
        )

        # Act
        prediction_jobs = self.service._get_prediction_jobs_query_results(query="")

        # Assert
        assert isinstance(prediction_jobs, list)
        assert isinstance(prediction_jobs[0], PredictionJobDataClass)
        assert len(prediction_jobs) == 1
        assert prediction_jobs[0].name == "Neerijnen"
