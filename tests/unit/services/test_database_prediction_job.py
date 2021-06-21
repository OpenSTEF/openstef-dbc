# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from ktpbase.services.prediction_job import PredictionJob

@patch("openstf_dbc.services.predictions._DataInterface")
class TestPredictionJob(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.service = PredictionJob()

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
