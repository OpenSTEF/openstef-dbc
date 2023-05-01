# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
from typing import Union, List
import datetime
import re

from datetime import timedelta

import pandas as pd
import structlog
from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.services.systems import Systems
from openstef_dbc.utils import process_datetime_range


class Ems:
    def __init__(self) -> None:
        self.logger = structlog.get_logger(self.__class__.__name__)

    def get_load_sid(
        self,
        sid: Union[List[str], str],
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str,
        aggregated: bool = True,
        average_output: bool = False,
        include_n_entries_column=False,
    ) -> pd.DataFrame:
        """Get the load for a single or multiple system id's.

            Get Measurements for given sid or list of sids. If no result is found,
            return empty dataframe.

        Args:
            sid(Union[str, List[str]]): System id's
            datetime_start (datetime): Start datetime.
            datetime_end (datetime): End datetime.
            forecast_resolution (str): The forecast resolution, for example '15T'
            aggregated (boolean): Should the results be aggregated per sid or not.
            average_output:

        Returns:
            - pd.DataFrame(index=datetimeIndex, columns)
            if aggregated: columns = [load, nEntries], else: columns = sid"""

        # Process datetimes (rounded, timezone, frequency) and generate index
        datetime_start, datetime_end, _ = process_datetime_range(
            start=datetime_start,
            end=datetime_end,
            freq=forecast_resolution,
        )
        # Define empty bind_params dict for query parametrization
        bind_params = {}

        # Convert sid to list
        if type(sid) is str:
            sid = [sid]

        # Prepare sid query string
        if len(sid) == 1:
            bind_params[f"param_sid_0"] = sid[0]
            sidsection = f'r.system == "{bind_params["param_sid_0"]}"'
        else:
            # Create parameters for each sid
            for i in range(len(sid)):
                bind_params[f"param_sid_{i}"] = sid[i]

            # Build parameters in query
            section = " or r.system == ".join(
                ['"' + s + '"' for s in list(bind_params.values())]
            )
            sidsection = f"r.system == {section}"

        bind_params.update(
            {
                "dstart": datetime_start.isoformat(),
                "dend": datetime_end.isoformat(),
            }
        )

        # Prepare query
        if aggregated:
            forecast_resolution_timedelta = timedelta(
                minutes=int(forecast_resolution[:-1])
            )
            # Extending the range is necessary to make sure the final timestamps are also in
            # the reponse after aggregations.
            bind_params["dend"] = (
                datetime_end + 2 * forecast_resolution_timedelta
            ).isoformat()
            query = f"""
                data = from(bucket: "realised/autogen") 
                    |> range(start: {bind_params['dstart']}, stop: {bind_params['dend']}) 
                    |> filter(fn: (r) => r._measurement == "power")
                    |> filter(fn: (r) => r._field == "output")
                    |> filter(fn: (r) => {sidsection})
                    |> aggregateWindow(every: {forecast_resolution.replace("T", "m")}, fn: mean)

                data
                    |> group() |> aggregateWindow(every: {forecast_resolution.replace("T", "m")}, fn: sum)
                    |> yield(name: "load")

                data
                    |> group() |> aggregateWindow(every: {forecast_resolution.replace("T", "m")}, fn: count)
                    |> yield(name: "nEntries")
            """
        else:
            query = f"""
                from(bucket: "realised/autogen") 
                    |> range(start: {bind_params['dstart']}, stop: {bind_params['dend']}) 
                    |> filter(fn: (r) => r._measurement == "power")
                    |> filter(fn: (r) => r._field == "output")
                    |> filter(fn: (r) => {sidsection})
                    |> pivot(rowKey:["_time"], columnKey: ["system"], valueColumn: "_value")
            """

        # Query load
        result_raw = _DataInterface.get_instance().exec_influx_query(query, bind_params)

        if isinstance(result_raw, pd.DataFrame) and result_raw.empty:
            self.logger.warning(
                f"Probably no load data avalailable for the following system(s): \n {sid} \n"
                f"The following query yields an empty dataframe: {query}"
            )
            return pd.DataFrame()

        if aggregated:
            result = pd.concat(
                [
                    result_raw[0][["_time", "_value"]].rename(
                        columns={"_value": "load"}
                    ),
                    result_raw[1][["_value"]].rename(columns={"_value": "nEntries"}),
                ],
                axis=1,
            ).set_index("_time")

            # Shifting the index two timeperiods back, because flux takes the right instead
            # of the left boundary when aggregating over a time window. In the flux query,
            # two aggregations are performed over a `forecast_resolution`-sized time window.
            result = result.shift(periods=-2, freq=forecast_resolution)
            # The first two rows are dropped, since their timestamps are before the `datetime_start`.
            result = result.iloc[2:, :]

            result = result.dropna()
            if average_output:
                result["load"] = result["load"] / result["nEntries"]

            outputcols = ["load"]

            if include_n_entries_column:
                outputcols.append("nEntries")

            return result[outputcols]
        else:
            result = result_raw.set_index("_time")[sid]
            result = result.resample(forecast_resolution).mean()
            return result

    def get_load_pid(
        self,
        pid: int,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = "15T",
        aggregated: bool = True,
        ignore_factor: bool = False,
    ) -> pd.DataFrame:
        """Get load(s) for a given prediction job id.

        Retrieve the load for all systems which belong to a given prediction job id. The
        loads will always be multiplied by both the `polarity` as well as the `factor`
        of any given system. Except when `ignore_factor` is set to True, in that case
        the load will not be multiplied  with the `factor`.

        Aggregated will return a single column dataframe. While non aggregated
        will return a multi column dataframe (one column per system).

        NOTE: if the polarity is set to `0` a positive polarity is assumed.

        Args:
            pid (int): id of the prediction job
            datetime_start (str): Datetime start range
            datetime_end (str): Datetime end range
            forecast_resolution: timeresolution of result in pd.resample() format
            aggregated (bool): Should the result for multiple sid's be aggregated?
                Default: True
            ignore_factor (bool): When set to True, the `factor` will not be applied.
                Default: False

        Returns:
            (pd.DataFrame): Load
        """

        # Process datetimes (rounded, timezone, frequency) and generate index
        datetime_start, datetime_end, _ = process_datetime_range(
            start=datetime_start,
            end=datetime_end,
            freq=forecast_resolution,
        )

        # Use optimized load retrieval if possible
        if aggregated and not ignore_factor:
            return self._get_load_pid_optimized(pid, datetime_start, datetime_end)

        # Get systems that belong to this prediction
        systems = Systems().get_systems_by_pid(pid)

        if systems.empty:
            raise ValueError(f"No systems found for given pid ({pid}).")

        # obtain load for all systems
        systems_load = self.get_load_sid(
            list(systems.system_id),
            datetime_start,
            datetime_end,
            forecast_resolution,
            aggregated=False,
        )

        # if load is empty, raise a warning and return empty dataframe
        if len(systems_load) == 0:
            self.logger.warning("No load data retrieved. Returning empty dataframe")
            return pd.DataFrame()

        # Check if all requested systems have a historic load, otherwise, give a warning
        # and ignore 'missing' systems
        missing_systems = [
            x for x in systems.system_id if x not in systems_load.columns
        ]
        num_missing_systems = len(missing_systems)
        if num_missing_systems > 0:
            msg = (
                f"There is/are {num_missing_systems} ({', '.join(missing_systems)}) "
                f"system(s) without a load for pid {pid}, ignore this/these system(s)."
            )
            self.logger.warning(
                msg,
                pid=pid,
                num_missing_systems=num_missing_systems,
                missing_systems=missing_systems,
            )
            systems = systems[~systems.system_id.isin(missing_systems)]

        # Apply positive or negative polariy to systems
        for system_id in systems.system_id:
            polarity = systems.loc[systems.system_id == system_id].polarity.iloc[0]
            if polarity == 0:
                self.logger.warning(
                    "Polarity not set use 1 by default", system_id=system_id
                )
                polarity = 1
            systems_load[system_id] *= polarity

        if ignore_factor is False:
            # apply predictions_systems factor -> should we add or subtract this load
            for system_id in systems.system_id:
                factor = systems.loc[systems.system_id == system_id].factor.iloc[0]
                systems_load[system_id] *= factor

        # if aggregated is False return non aggregated dataframe
        if aggregated is False:
            return systems_load

        # if aggregated is true all normal and load correction systems will be added
        total_load = pd.DataFrame()

        # aggregrate the load by adding all system loads
        total_load["load"] = systems_load.sum(axis=1)

        return total_load

    def _get_load_pid_optimized(
        self,
        pid: int,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = "15T",
    ) -> pd.DataFrame:
        """Gets load data for a pid.
        This method optimizes the way it retrieves data and is therefore less flexible as get_load_pid.
        It is however much faster for prediction jobs with a large amount of sid's.

        Args:
            pid (int): id of the prediction job
            datetime_start (str): Datetime start range
            datetime_end (str): Datetime end range
            forecast_resolution: timeresolution of result in pd.resample() format

        Returns:
            (pd.DataFrame): Load

        """
        # Get systems that belong to this prediction
        systems = Systems().get_systems_by_pid(pid)

        # Set factor or polarity of 0 to 1
        systems.loc[systems["factor"] == 0, "factor"] = 1
        systems.loc[systems["polarity"] == 0, "polarity"] = 1
        systems["effective_factor"] = systems["factor"] * systems["polarity"]

        # Build dict with unique effective factors as keys and lists of corresponding systems as values
        effective_factors = {
            effective_factor: systems[systems["effective_factor"] == effective_factor]
            for effective_factor in set(systems["effective_factor"])
        }

        combined_load = pd.DataFrame()
        # Retrieve load for each unique effective_factor
        for effective_factor, sids in effective_factors.items():
            load = (
                self.get_load_sid(
                    sids.system_id.to_list(),
                    datetime_start,
                    datetime_end,
                    forecast_resolution,
                    aggregated=True,
                )
                * effective_factor
            )

            # Combine individual results
            load = load.rename(columns=dict(load=effective_factor))

            # Use merge so potential gaps in the individual timeseries do not cause issues
            combined_load = combined_load.merge(
                load, left_index=True, right_index=True, how="outer"
            )

        #  Return sum all load columns
        return pd.DataFrame(combined_load.sum(axis=1).rename("load"))

    @staticmethod
    def _check_influx_group_by_time_statement(statement: str) -> None:
        # Validate forecast resolution to prevent injections
        if not re.match(
            r"[0-9]+([unµm]?s|m|h|d|w)",
            statement,
        ):
            raise ValueError("Forecast resolution does not have the allowed format!")
