from datetime import datetime
from unittest import mock

import pandas as pd

from openstf_dbc.database import DataBase
from openstf_dbc.data_interface import _DataInterface

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

df = (
    pd.DataFrame.from_dict(
        {
            "subsample": 1,
            "min_child_weight": 2,
            "max_depth": 3,
            "gamma": 4,
            "colsample_bytree": 5,
            "silent": 6,
            "objective": 7,
            "eta": 8,
        },
        orient="index",
    )
    .reset_index()
    .rename(columns={"index": "name", 0: "id"})
)

data_interface_mock = mock.MagicMock()
get_instance_mock = mock.MagicMock(return_value=data_interface_mock)

# Test conversion to proper dataformat
@mock.patch.object(_DataInterface, "get_instance", get_instance_mock)
def test_mock_query(*args):
    params = dict()
    params["subsample"] = 0.9

    created = datetime.utcnow().replace(second=0, microsecond=0)
    formatdict = dict(created=created, pid=pj["id"], subsample=params["subsample"])

    data_interface_mock.exec_sql_query.return_value = df

    db = DataBase()
    db.write_hyper_params(pj, params)

    # TODO this is probably too specific (which makes it brittle),
    # the exact formatting should not make the tests fail if the effect is the
    # same
    expected_call = " ".join(
        """
    INSERT INTO
        `hyper_param_values` (prediction_id, hyper_params_id, value, created)
    VALUES ("{pid}", "1", "{subsample}", "{created}")
    ON DUPLICATE KEY UPDATE
        value=VALUES(value),
        created=VALUES(created)
    """.format(
            **formatdict
        ).split()
    )

    actual_call = " ".join(data_interface_mock.exec_sql_write.call_args[0][0].split())

    assert actual_call == expected_call


if __name__ == "__main__":
    test_mock_query()
    print("Passed!")
