# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <openstef@lfenergy.org>
#
# SPDX-License-Identifier: MPL-2.0
import unittest
from datetime import datetime, timedelta

from openstef_dbc.services.ems import Ems


class TestEMS(unittest.TestCase):
    @unittest.skip
    def test_ems(self):
        # Check to compare classical get_load_pid with get_load_pid_optimized
        pid = 454
        start = datetime.utcnow() - timedelta(days=7)
        end = datetime.utcnow()

        load = Ems().get_load_pid(
            pid=pid, datetime_start=start, datetime_end=end, forecast_resolution="15min"
        )
        load_optimized = Ems()._get_load_pid_optimized(
            pid=pid, datetime_start=start, datetime_end=end, forecast_resolution="15min"
        )

        print(f"get_load_pid :{load}")

        print(f"get_load_pid_optimized :{load_optimized}")
