# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

# -*- coding: utf-8 -*-
import unittest

from openstf_dbc.services.model_specifications import ModelSpecificationDataClass


class TestModelSpecifications(unittest.TestCase):
    def test_dataclass(self):
        model_specifications = {
            "id": 307,
            "hyper_params": {
                "subsample": 0.9650102355823993,
                "min_child_weight": 3,
                "max_depth": 6,
                "gamma": 0.1313691782115394,
                "colsample_bytree": 0.8206844265155975,
                "silent": 1,
                "objective": "reg:squarederror",
                "eta": 0.010025843216782565,
                "training_period_days": 90,
            },
            "feature_names": [
                "clouds",
                "humidity",
                "pressure",
                "radiation",
                "rain",
                "temp",
                "windspeed",
            ],
        }
        ms_dataclass = ModelSpecificationDataClass(**model_specifications)
        self.assertIsInstance(ms_dataclass, ModelSpecificationDataClass)


if __name__ == "__main__":
    unittest.main()
