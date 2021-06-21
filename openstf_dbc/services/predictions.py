# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime, timedelta

import pandas as pd
import re

from openstf_dbc.data_interface import _DataInterface


class Predictions:
    def get_predicted_load(self, pj, start_time=None, end_time=None):
        """Get historic load predictions for given pid.

        This functions ignores component predictions that result from energy splitting.

        Args:
            pj (dict): Prediction job
            start_time (datetime): Start time  to retrieve the historic load prediction.
            end_time (datetime): End timeto retrieve the historic load prediction.

        Returns:
            pandas.Series: Forecast column with the predicted values

        """
        # Apply default parameters if none are provided
        if start_time is None:
            start_time = datetime.utcnow()
        if end_time is None:
            end_time = datetime.utcnow() + timedelta(days=2)

        query = """
            SELECT mean("forecast") as forecast, mean("stdev") as stdev
            FROM forecast_latest..prediction
            WHERE (
                "pid" = '{}' AND
                "type" != 'est_demand'
                AND "type" != 'est_pv'
                AND "type" != 'est_wind'
            ) AND time >= '{}' AND time < '{}'
            GROUP BY time(15m)
        """.format(
            pj["id"], start_time, end_time
        )

        # Query the database
        result = _DataInterface.get_instance().exec_influx_query(query)

        # Return result
        if "prediction" in result:
            return result["prediction"]
        else:
            return pd.Series()

    def get_predicted_load_tahead(
        self, pj, start_time=None, end_time=None, t_ahead=None, component=False
    ):
        """Get historic load predictions for given pid and t_ahead.

        This functions ignores component predictions that result from energy splitting.

        Args:
            pj (dict): Prediction job
            start_time (datetime): Start time  to retrieve the historic load prediction.
            end_time (datetime): End timeto retrieve the historic load prediction.
            t_ahead (String, list of strings or None):
                Specifies specific t_ahead(s) for which to retrieve historic predictions,
                if none all available t_aheads will be returned.

        Returns:
            pandas.DataFrame: object with a column for each requested t_ahead

        """
        # Apply default parameters if none are provided
        if start_time is None:
            start_time = datetime.utcnow() - timedelta(days=1)
        if end_time is None:
            end_time = datetime.utcnow()

        # If no t_ahead are provided ask influx for all t_ahead available
        if t_ahead is None:
            if component:
                query = """
                    SELECT mean("forecast_solar") as forecast, mean("stdev") as stdev
                    FROM forecast_latest..prediction_tAheads
                    WHERE ("pid" = '{}' AND "type" = '{}') AND time >= '{}' AND time < '{}'
                    GROUP BY time(15m), "tAhead"
                """.format(
                    pj["id"], "demand", start_time, end_time
                )
            else:
                query = """
                    SELECT mean("forecast") as forecast, mean("stdev") as stdev
                    FROM forecast_latest..prediction_tAheads
                    WHERE ("pid" = '{}' AND "type" = '{}') AND time >= '{}' AND time < '{}'
                    GROUP BY time(15m), "tAhead"
                """.format(
                    pj["id"], "demand", start_time, end_time
                )

        # For a selection of t_aheads a custom query is generated
        else:
            # If string convert to single item list and continue
            if isinstance(t_ahead, str):
                t_ahead = [t_ahead]

            # Raise exception when input is not a list of strings or a string.
            elif not all(isinstance(item, str) for item in t_ahead) and not isinstance(
                t_ahead, str
            ):
                raise ValueError("Could not interpret t_ahead argument!")

            # Placeholder string
            t_aheads = ""
            # Loop over requested t_aheads and add them to the query
            for item in t_ahead:
                # Get rid of hour symbols
                item = re.sub("[hH]", "", item)
                # Make string for this t_ahead
                t_aheads = t_aheads + """ "tAhead" = '{}' OR""".format(str(item) + ".0")

            # Get rid of last OR
            t_aheads = t_aheads[0:-2]

            # Make query for a selection of t_aheads
            query = """
                SELECT mean("forecast") as forecast, mean("stdev") as stdev
                FROM forecast_latest..prediction_tAheads
                WHERE ("pid" = '{}' AND "type" = '{}' AND ({})) AND time >= '{}' AND time < '{}'
                GROUP BY time(15m), "tAhead"
            """.format(
                pj["id"], "demand", t_aheads, start_time, end_time
            )

        # Query the database
        result = _DataInterface.get_instance().exec_influx_query(query)

        # Convert to pandas DataFrame with a column for each tAhead
        predicted_load = pd.DataFrame()

        for t_ahead in list(result):
            h_ahead = str(t_ahead[1][0][1])
            renames = dict(forecast=f"forecast_{h_ahead}h", stdev=f"stdev_{h_ahead}h")
            predicted_load = predicted_load.merge(
                result[t_ahead].rename(columns=renames),
                how="outer",
                left_index=True,
                right_index=True,
            )

        # Return result
        return predicted_load
