# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <openstef@lfenergy.org>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from unittest.mock import patch
import pandas as pd
from openstef.data_classes.prediction_job import PredictionJobDataClass
from pydantic.v1.error_wrappers import ValidationError
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
            "train_horizons_minutes": None,
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
        # Arrange
        pj_dict = self.prediction_job

        # Act
        pj_object = self.service._create_prediction_job_object(pj_dict)

        # Assert
        self.assertEqual(pj_object.__getitem__("id"), pj_dict["id"])

    def test_create_prediction_job_object_set_item_afterwards(self):
        # Arrange
        pj_dict = self.prediction_job

        # Act
        pj_object = self.service._create_prediction_job_object(pj_dict)
        pj_object.__setitem__("id", 50)

        # Assert
        self.assertEqual(pj_object.__getitem__("id"), 50)

    def test_create_prediction_job_object_cannot_set_nonexistent_attribute(self):
        # Arrange
        pj_dict = self.prediction_job

        # Act
        pj_object = self.service._create_prediction_job_object(pj_dict)

        # Assert
        with self.assertRaises(AttributeError):
            pj_object.__setitem__("non_existing", "can't")

    def test_create_prediction_job_object_with_train_horizons(self):
        # Arrange
        pj_dict = self.prediction_job
        train_horizons_minutes_list = [15, 2880, 15000]
        train_horizons_minutes_str = "[15, 2880, 15000]"
        pj_dict["train_horizons_minutes"] = train_horizons_minutes_str

        # Act
        pj_object = self.service._create_prediction_job_object(pj_dict)

        # Assert
        self.assertEqual(
            pj_object.__getitem__("train_horizons_minutes"), train_horizons_minutes_list
        )

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

    @patch("openstef_dbc.services.prediction_job._DataInterface")
    @patch(
        "openstef_dbc.services.prediction_job.PredictionJobRetriever._add_description_to_prediction_jobs"
    )
    def test__get_prediction_jobs_query_results(
        self,
        add_description_mock,
        data_interface_mock,
    ):
        # Arrange
        data_interface_mock.get_instance.return_value.exec_sql_query.return_value = (
            pd.DataFrame([self.prediction_job])
        )
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
