# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from openstf_dbc.services.prediction_job import PredictionJob


@patch("openstf_dbc.services.predictions._DataInterface")
class TestPredictionJob(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.service = PredictionJob()

    def test_build_get_prediction_jobs_query(self):
        kwargs = {
            "pid": 123,
            "model_type": "xgb",
            "active": 1,
            "only_ato": True,
            "external_id": "e179c450-30cc-4fb8-a9c8-1cd6feee2cbd",
            "limit": 999,
        }
        query = PredictionJob.build_get_prediction_jobs_query(**kwargs)
        for key, value in kwargs.items():
            if key == "only_ato":
                self.assertTrue("ATO" in query)
                continue
            self.assertTrue(value in query)

    def test_get_featureset(self, data_interface_mock):
        featureset_names = self.service.get_featureset_names()
        for name in featureset_names:
            featureset = self.service.get_featureset(name)
            self.assertEqual(type(featureset), list)

    def test_get_featuresets(self, data_interface_mock):
        self.assertEqual(type(self.service.get_featuresets()), dict)

    def test_get_featureset_names(self, data_interface_mock):
        self.assertEqual(type(self.service.get_featureset_names()), list)


if __name__ == "__main__":
    unittest.main()
