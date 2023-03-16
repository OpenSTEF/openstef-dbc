# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
import pytz
from typing import Union, List
from datetime import datetime, timedelta

import pandas as pd
import re

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.utils import parse_influx_result


class Predictions:
    def get_predicted_load(
        self,
        pj: dict,
        start_time: datetime = None,
        end_time: datetime = None,
    ) -> pd.Series:
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

        bind_params = {
            "pid": str(pj["id"]),
            "_start": start_time.astimezone(pytz.UTC),
            "_stop": end_time.astimezone(pytz.UTC),
        }

        query = f"""
            from(bucket: "forecast_latest/autogen")
                |> range(start: {bind_params["_start"].strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {bind_params["_stop"].strftime('%Y-%m-%dT%H:%M:%SZ')}) 
                |> filter(fn: (r) => 
                    r._measurement == "prediction")
                |> filter(fn: (r) => 
                    r._field == "forecast" or r._field == "stdev") 
                |> filter(fn: (r) => 
                    r.type != "est_demand" and  r.type != "est_wind" and  r.type != "est_pv") 
                |> filter(fn: (r) => r.pid == "{bind_params["pid"]}")
                |> aggregateWindow(every: {pj["resolution_minutes"]}m, fn: mean)
        """

        # Query the database
        result = _DataInterface.get_instance().exec_influx_query(query, bind_params)

        # For multiple Fields a list is returned.
        if isinstance(result, list):
            result = pd.concat(result)[["_value", "_field", "_time"]]

        # Return result
        if not result.empty:
            # Shifting is needed to output the same as with the old influx client
            return parse_influx_result(result).shift(-15, freq="T")
        else:
            return pd.Series()

    def get_predicted_load_tahead(
        self,
        pj: dict,
        start_time: datetime = None,
        end_time: datetime = None,
        t_ahead: Union[List[str], str] = None,
        component: bool = False,
    ) -> pd.DataFrame:
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
                bind_params = {
                    "pid": str(pj["id"]),
                    "_start": start_time.astimezone(pytz.UTC),
                    "_stop": end_time.astimezone(pytz.UTC),
                }
                query = f"""
                    from(bucket: "forecast_latest/autogen")
                        |> range(start: {bind_params["_start"].strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {bind_params["_stop"].strftime('%Y-%m-%dT%H:%M:%SZ')}) 
                        |> filter(fn: (r) => 
                            r._measurement == "prediction_tAheads")
                        |> filter(fn: (r) => 
                            r._field == "forecast_solar" or r._field == "stdev") 
                        |> filter(fn: (r) => 
                            r.type != "est_demand" and  r.type != "est_wind" and  r.type != "est_pv") 
                        |> filter(fn: (r) => r.pid == "{bind_params["pid"]}")
                        |> aggregateWindow(every: {pj["resolution_minutes"]}m, fn: mean)
                """
            else:
                bind_params = {
                    "pid": str(pj["id"]),
                    "_start": start_time,
                    "_stop": end_time,
                }
                query = f"""
                    from(bucket: "forecast_latest/autogen")
                        |> range(start: {bind_params["_start"].strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {bind_params["_stop"].strftime('%Y-%m-%dT%H:%M:%SZ')}) 
                        |> filter(fn: (r) => 
                            r._measurement == "prediction_tAheads")
                        |> filter(fn: (r) => 
                            r._field == "forecast" or r._field == "stdev") 
                        |> filter(fn: (r) => 
                            r.type != "est_demand" and  r.type != "est_wind" and  r.type != "est_pv") 
                        |> filter(fn: (r) => r.pid == "{bind_params["pid"]}")
                        |> aggregateWindow(every: {pj["resolution_minutes"]}m, fn: mean)
                """

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
            bind_params = {
                "pid": str(pj["id"]),
                "_start": start_time.astimezone(pytz.UTC),
                "_stop": end_time.astimezone(pytz.UTC),
            }
            t_aheads_section = '" or r.taheads == "'.join(t_aheads)

            query = f"""
                from(bucket: "forecast_latest/autogen")
                    |> range(start: {bind_params["_start"].strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {bind_params["_stop"].strftime('%Y-%m-%dT%H:%M:%SZ')}) 
                    |> filter(fn: (r) => 
                        r._measurement == "prediction_tAheads")
                    |> filter(fn: (r) => 
                        r._field == "forecast" or r._field == "stdev") 
                    |> filter(fn: (r) => 
                        r.type != "est_demand" and  r.type != "est_wind" and  r.type != "est_pv") 
                    |> filter(fn: (r) => r.pid == "{bind_params["pid"]}")
                    |> filter(fn: (r) => r.taheads == "{t_aheads_section}")
                    |> aggregateWindow(every: {pj["resolution_minutes"]}m, fn: mean)
            """

        # Query the database
        result = _DataInterface.get_instance().exec_influx_query(query, bind_params)

        # For multiple Fields a list is returned.
        if isinstance(result, list):
            result = pd.concat(result)[["_value", "_field", "_time"]]

        # Return result
        if not result.empty:
            # Shifting is needed to output the same as with the old influx client
            result = parse_influx_result(result, aditional_indices=["tAhead"]).shift(
                -15, freq="T"
            )
        else:
            return pd.Series()

        # Convert to pandas DataFrame with a column for each tAhead
        predicted_load = result.pivot(columns="tAhead")

        predicted_load.columns = [
            "_".join(col) + "h" for col in predicted_load.columns.values
        ]

        # Return result
        return predicted_load
