# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <openstef@lfenergy.org>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from datetime import datetime
import pandas as pd
from unittest.mock import MagicMock, patch

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

        data_interface_mock.get_instance.return_value.exec_influx_write.return_value = (
            True
        )

        # Arange
        writer = Write()
        example_data = pd.DataFrame(
            index=pd.to_datetime(
                ["2023-01-01 10:00:00+00:00", "2023-01-01 10:15:00+00:00"]
            ),
            data=dict(
                forecast=[1.1, 2.0],
                pid=[1, 1],
                stdev=[0, 0],
                description=["test", "test"],
                customer=["example", "example"],
                quality=["actual", "actual"],
            ),
        )
        data_interface_mock()

        # Act
        writer.write_forecast(data=example_data, t_ahead_series=True)

        # Assert
        assert writer

    @patch("openstef_dbc.services.write._DataInterface")
    def test_write_weather_forecast_data(self, MockDataInterface):
        """Test happy flow of writing weather forecasts."""

        mock_instance = MagicMock()
        MockDataInterface.get_instance.return_value = mock_instance
        MockDataInterface.get_instance.return_value.exec_influx_write.return_value = (
            True
        )
        # Arange
        writer = Write()
        example_data = pd.DataFrame(
            index=pd.to_datetime(
                ["2023-01-01 10:00:00+00:00", "2023-01-01 10:15:00+00:00"]
            ),
            data=dict(
                input_city=["city1", "city1"],
                temp=[23.1, 23.0],
                windspeed=[1000, 1100],
            ),
        )
        example_forecast_created_time = pd.to_datetime("2023-01-01 09:00:00+00:00")
        table_name = "weather_forecast"

        # Act
        writer.write_weather_forecast_data(
            data=example_data,
            source="test_source",
            table=table_name,
            dbname="test_db",
            forecast_created_time=example_forecast_created_time,
        )

        # Assert
        # write_weather_forecast_data should write in two buckets
        self.assertEqual(MockDataInterface.get_instance.call_count, 2)

        # Verifying call arguments (table name and number of rows of inserted dataframe)
        calls = mock_instance.exec_influx_write.call_args_list

        # First call
        first_call = calls[0]
        first_call_args = first_call[0][0]
        first_call_kwargs = first_call[1]
        assert first_call_kwargs["measurement"] == table_name
        assert first_call_args.shape[0] == 2  # whatever default desired_t_ahead

        # Second call
        second_call = calls[1]
        second_call_args = second_call[0][0]
        second_call_kwargs = second_call[1]
        assert second_call_kwargs["measurement"] == (table_name + "_tAhead")
        assert second_call_args.shape[0] == 1  # only default desired_t_ahead
