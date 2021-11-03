# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

# -*- coding: utf-8 -*-
import unittest

from openstf_dbc.data.featuresets import FEATURESET_NAMES
from openstf_dbc.services.model_specifications import ModelSpecificationRetriever


class TestGetFeatureSet(unittest.TestCase):
    def test_get_featureset(self):
        for name in FEATURESET_NAMES:
            featureset = ModelSpecificationRetriever().get_featureset(name)
            if name == "N":
                self.assertIsInstance(featureset, type(None))
            else:
                self.assertIsInstance(featureset, list)

    def test_get_featureset_wrong_name(self):
        with self.assertRaises(KeyError):
            ModelSpecificationRetriever().get_featureset("wrong_name")


if __name__ == "__main__":
    unittest.main()
