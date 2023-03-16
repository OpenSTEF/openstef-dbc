# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta

from openstef_dbc.services.predictions import Predictions

pj = {
    "id": 298,
    "description": "Rzb_10-6i_V601_P+Rzb_10-6i_V602_P+Rzb_10_V145_P+Rzb_10_V148_P+Rzb_10_V154_P+Rzb_10_V164_P",
    "sid": "Rzb_10-6i_V601_P",
    "cid": 394,
    "typ": "demand",
    "model": "xgb",
    "active": 1,
    "resolution_minutes": 15,
    "horizon_minutes": 2880,
}


@patch("openstef_dbc.services.predictions._DataInterface")
class TestPredictions(unittest.TestCase):
    def test_get_predicted_load(self, data_interface_mock):
        pass

    def test_get_predicted_load_tahead(self, data_interface_mock):
        start_time = datetime.utcnow() - timedelta(days=7)
        end_time = datetime.utcnow()
        predictions_service = Predictions()
        predictions_service.get_predicted_load_tahead(
            pj=pj,
            start_time=start_time,
            end_time=end_time,
            t_ahead=None,
            component=False,
        )
        # get the _DataInterface mock (returned from get_instance)
        di = data_interface_mock.get_instance.return_value
        # Make sure exec_influx_query was called
        self.assertEqual(di.exec_influx_query.call_count, 1)
        # With one argument (the query)
        args, kwargs = di.exec_influx_query.call_args
        self.assertEqual(len(args), 2)
        query = args[0]
        should_contain = [
            "forecast",
            "stdev",
            "prediction_tAheads",
            "demand",
        ]
        should_not_containe = ["forecast_solar"]

        for contains in should_contain:
            self.assertTrue(contains in query)

        for not_contains in should_not_containe:
            self.assertFalse(not_contains in query)

        # TODO tests all different flow start_time, end_time is None, t_ahead is not None
        # components is True and any combination of the above.


if __name__ == "__main__":
    unittest.main()
