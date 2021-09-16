# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from unittest import mock

import pandas as pd
from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.database import DataBase

# Define tests data
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
    "value": [0.9, 4, 5, 0.07518330, 0.40687857, 1, "reg:linear", 500, 0.9, 4],
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

config = mock.MagicMock()
config.mysql.username = "username"
config.mysql.password = "password"
config.mysql.host = "host"
config.mysql.port = 123
config.mysql.database_name = "database_name"
config.influxdb.username = "username"
config.influxdb.password = "password"
config.influxdb.host = "host"
config.influxdb.port = 123


data_interface_mock = mock.MagicMock()
get_instance_mock = mock.MagicMock(return_value=data_interface_mock)


# Test conversion to proper dataformat
@mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
def test_proper_data_format(*args):
    """This tests tests if the values are correctly converted from a pandas dataframe to a dictionary"""
    data_interface_mock.exec_sql_query.return_value = db_result_good_parameters
    db = DataBase(config)
    params = db.get_hyper_params(pj)
    assert params == good_hyper_params


# Test fall back on default parameters


@mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
def test_empty_data_frame(*args):
    """This tests tests if the default parameters are returned if the database returns an empty dataframe"""
    data_interface_mock.exec_sql_query.return_value = empty_data_frame
    db = DataBase(config)
    params = db.get_hyper_params(pj)
    assert params == {}


# Test if query went wrong
@mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
def test_error_in_query(*args):
    """This tests tests if the default hyperparameters are returned in case of an error"""
    data_interface_mock.exec_sql_query.return_value = None
    db = DataBase(config)
    params = db.get_hyper_params(pj)
    assert params == {}


if __name__ == "__main__":
    tests = [eval(item) for item in dir() if item[0 : len("test_")] == "test_"]
    for test in tests:
        test()
        print("passed")
