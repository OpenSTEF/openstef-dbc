# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
from datetime import datetime
import pandas as pd
from unittest.mock import patch

# from openstef_dbc.services.systems import Systems
from openstef_dbc.services.ems import Ems as EmsService

datetime_start = datetime.fromisoformat("2019-01-01 10:00:00")
datetime_end = datetime.fromisoformat("2019-01-01 10:15:00")


@patch("openstef_dbc.services.ems.Systems.get_systems_by_pid")
@patch("openstef_dbc.services.ems.Ems.get_load_sid")
class TestEmsService(unittest.TestCase):
    def setUp(self) -> None:
        self.systems = pd.DataFrame(
            [
                {"system_id": "NrynRS_10-G_V12_P", "polarity": 1, "factor": 1},
                {"system_id": "NrynRS_10-G_V13_P", "polarity": 1, "factor": 1},
                {"system_id": "NrynRS_10-G_V14_P", "polarity": 1, "factor": 1},
            ]
        )
        self.systems_load = pd.DataFrame(
            index=pd.to_datetime([datetime_start, datetime_end]),
            data={
                "NrynRS_10-G_V12_P": [15.0, 20.0],
                "NrynRS_10-G_V13_P": [11.0, 13.0],
                "NrynRS_10-G_V14_P": [9.0, 7.0],
            },
        )
        return super().setUp()

    def test_get_load_pid_polarity_positive(self, get_load_sid_mock, get_systems_mock):
        """Get load pid for systems with a positive polarity"""
        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(123, datetime_start, datetime_end)

        expected_load = pd.DataFrame(data={"load": self.systems_load.sum(axis=1)})

        pd.testing.assert_frame_equal(load, expected_load)

    def test_get_load_pid_polarity_negative(self, get_load_sid_mock, get_systems_mock):
        """Get load pid for systems with a negative polarity"""
        # make polarity negative
        self.systems.polarity = -1

        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(123, datetime_start, datetime_end)

        expected_load = pd.DataFrame(data={"load": -1 * self.systems_load.sum(axis=1)})

        pd.testing.assert_frame_equal(load, expected_load)

    def test_get_load_pid_polarity_mixed(self, get_load_sid_mock, get_systems_mock):
        """Get load pid for systems with a negative polarity"""
        # make polarity of first system negative
        self.systems.at[0, "polarity"] = -1
        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(123, datetime_start, datetime_end)

        # multiply the first column (first system) by -1
        self.systems_load.iloc[:, 0] *= -1
        expected_load = pd.DataFrame(
            data={"load": self.systems_load.sum(axis=1)}
        ) - pd.DataFrame(data={"load": self.systems_load.sum(axis=1)})

        pd.testing.assert_frame_equal(load, expected_load)

    def test_get_load_pid_factor_positive(self, get_load_sid_mock, get_systems_mock):
        """Get load pid for systems with a positive factor"""
        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(123, datetime_start, datetime_end)

        expected_load = pd.DataFrame(data={"load": self.systems_load.sum(axis=1)})

        pd.testing.assert_frame_equal(load, expected_load)

    def test_get_load_pid_factor_negative(self, get_load_sid_mock, get_systems_mock):
        """Get load pid for systems with a negative factor"""
        # make factor negative
        self.systems.factor = -1

        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(123, datetime_start, datetime_end)

        expected_load = pd.DataFrame(data={"load": -1 * self.systems_load.sum(axis=1)})

        pd.testing.assert_frame_equal(load, expected_load)

    def test_get_load_pid_factor_mixed(self, get_load_sid_mock, get_systems_mock):
        """Get load pid for systems with a negative factor"""
        # make polarity of first system negative
        self.systems.loc[0, "factor"] = -0.5
        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(123, datetime_start, datetime_end)

        expected_load = pd.DataFrame(
            data={"load": self.systems_load.sum(axis=1)}
        ) - 0.5 * pd.DataFrame(data={"load": self.systems_load.sum(axis=1)})

        pd.testing.assert_frame_equal(load, expected_load)

    def test_get_load_pid_non_aggregated_polarity_negative(
        self, get_load_sid_mock, get_systems_mock
    ):
        # make polarity negative
        self.systems.polarity = -1

        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(
            123, datetime_start, datetime_end, aggregated=False
        )
        expected_load = self.systems_load * -1
        pd.testing.assert_frame_equal(load, expected_load)

    def test_get_load_pid_non_aggregated_factor_negative(
        self, get_load_sid_mock, get_systems_mock
    ):
        # make polarity negative
        self.systems.factor = -1

        get_systems_mock.return_value = self.systems
        get_load_sid_mock.return_value = self.systems_load.copy()

        load = EmsService().get_load_pid(
            123, datetime_start, datetime_end, aggregated=False
        )
        # a negative factor should change the load
        pd.testing.assert_frame_equal(load, -1 * self.systems_load)

    def test_get_load_pid_non_aggregated_systems_without_historic_load(
        self, get_load_sid_mock, get_systems_mock
    ):
        """Test get_load_pid non aggregated with missing system.

        If systems for a given pid do not have a historic load, these systems
        should be ignored and not cause an exception.
        """

        get_systems_mock.return_value = self.systems
        # Do not return historic load for the first system
        get_load_sid_mock.return_value = self.systems_load.copy().iloc[:, 1:]

        load = EmsService().get_load_pid(
            123, datetime_start, datetime_end, aggregated=False
        )

        # check that the number of columns in the returned load is eqaul to the number
        # of columns in the get_load_sid mocked return value
        self.assertEqual(len(load), len(self.systems_load))
