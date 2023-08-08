# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from datetime import datetime
import pandas as pd
from unittest.mock import patch

# from openstef_dbc.services.systems import Systems
from openstef_dbc.services.write import Write

datetime_start = datetime.fromisoformat("2019-01-01 10:00:00")
datetime_end = datetime.fromisoformat("2019-01-01 10:15:00")



class TestWriteService(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    @patch("openstef_dbc.services.write._DataInterface")
    def test_write_forecast_tahead_happy_flow(self, data_interface_mock):
        """Test happy flow of writing forecasts. The actual writing is mocked"""
        data_interface_mock.get_instance.return_value.exec_influx_write.return_value = True
        
        # Arange
        writer = Write()
        example_data = pd.DataFrame(
            index=pd.to_datetime(['2023-01-01 10:00:00+00:00','2023-01-01 10:15:00+00:00']),
            data=dict(forecast=[1.1,2.0],
                      pid=[1,1],
                      stdev=[0,0],
                      description=['test','test'],
                      customer=['example','example'],
                      quality=['actual','actual'],
            )
        )
        data_interface_mock()
        
        writer.write_forecast(data=example_data,
                             t_ahead_series=True)
        
        assert writer
