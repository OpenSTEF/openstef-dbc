# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
import unittest
from unittest import mock
from datetime import datetime

import pandas as pd
from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.database import DataBase
from openstf_dbc.services.model_specifications import ModelSpecificationRetriever

data = {
    "name": [
        "silent",
        "eval",
        "gamma",
        "eta",
        "objective",
        "colsample_bytree",
        "max_depth",
        "n_estimators",
        "subsample",
        "min_child_weight",
    ],
    "value": [0.9, 4, 5, 0.07518330, 0.40687857, 1, "reg:linear", 500, 0.9, 4,],
}

db_result_good_parameters = pd.DataFrame.from_dict(data)

empty_data_frame = db_result_good_parameters.drop(db_result_good_parameters.index)

good_hyper_params = {
    "silent": 0.9,
    "eval": 4,
    "gamma": 5,
    "eta": 0.0751833,
    "objective": 0.40687857,
    "colsample_bytree": 1,
    "max_depth": "reg:linear",
    "n_estimators": 500,
    "subsample": 0.9,
    "min_child_weight": 4,
}

pj = {
    "id": 307,
    "typ": "demand",
    "model": "xgb",
    "horizon_minutes": 2880,
    "resolution_minutes": 15,
    "train_components": 1,
    "name": "Neerijnen",
    "lat": 51.8336647,
    "lon": 5.2137814,
    "sid": "NrynRS_10-G_V12_P",
    "created": "2019-04-05 12:08:23",
    "description": "NrynRS_10-G_V12_P+NrynRS_10-G_V13_P+NrynRS_10-G_V14_P+NrynRS_10-G_V15_P+NrynRS_10-G_V16_P+NrynRS_10-G_V17_P+NrynRS_10-G_V18_P+NrynRS_10-G_V20_P+NrynRS_10-G_V21_P+NrynRS_10-G_V22_P+NrynRS_10-G_V23_P+NrynRS_10-G_V24_P+NrynRS_10-G_V25_P",
}

data_interface_mock = mock.MagicMock()
get_instance_mock = mock.MagicMock(return_value=data_interface_mock)


class TestDatabaseGetHyperParams(unittest.TestCase):
    @mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
    def test_get_hyper_params_proper_data_format(self):
        """Tests if values are correctly converted from a dataframe to a dictionary."""
        data_interface_mock.exec_sql_query.return_value = db_result_good_parameters
        db = DataBase()
        params = ModelSpecificationRetriever().get_hyper_params(pj)
        assert params == good_hyper_params

    @mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
    def test_get_hyper_params_fallback_empty_data_frame(self):
        """Tests if default params are returned if db returns an empty dataframe."""
        data_interface_mock.exec_sql_query.return_value = empty_data_frame
        db = DataBase()
        params = ModelSpecificationRetriever().get_hyper_params(pj)
        assert params == {}

    @mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
    def test_get_hyper_params_error_in_query(self):
        """Tests if the default hyperparameters are returned in case of an error."""
        data_interface_mock.exec_sql_query.return_value = None
        db = DataBase()
        params = ModelSpecificationRetriever().get_hyper_params(pj)
        assert params == {}

    @mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
    def test_get_hyper_params_last_optimized_proper_data_format(self):
        """Tests if values are correctly converted from a dataframe to a dictionary."""
        last_data = {"last": ["2019-04-05 12:08:23"]}
        last_db_result = pd.DataFrame.from_dict(last_data)
        last_db_result['last'] = pd.to_datetime(last_db_result['last'])
        data_interface_mock.exec_sql_query.return_value = last_db_result

        last_pj = {"id": 307, "model": "xgb", "last": "2019-04-05 12:08:23"}
        db = DataBase()
        last_datetime = db.get_hyper_params_last_optimized(last_pj)
        time_format = "%Y-%m-%d %H:%M:%S"
        expected_last_datetime = datetime.strptime("2019-04-05 12:08:23", time_format)
        assert last_datetime == expected_last_datetime

    @mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
    def test_get_hyper_params_last_optimized_error_in_query(self):
        """Tests  if the last hyperparameters are returned in case of an error."""
        data_interface_mock.exec_sql_query.return_value = None
        db = DataBase()
        params = db.get_hyper_params_last_optimized(pj)
        expected_error_output = None
        assert params == expected_error_output


if __name__ == "__main__":
    unittest.main()
