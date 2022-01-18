# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
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
            # Escape forward slahes as inlfux cant handle them
            sid = sid.replace("/", "\/")
            sid = sid.replace("+", "\+")
            sid = [sid]

        # Prepare sid query string
        if len(sid) == 1:
            # Escape forward slahes as inlfux cant handle them
            sid[0] = sid[0].replace("/", "\/")
            sid[0] = sid[0].replace("+", "\+")
            bind_params[f"param_sid_0"] = sid[0]
            sidsection = '"system" = $param_sid_0'
        else:
            # Create parameters for each sid
            for i in range(len(sid)):

                # Escape forward slahes as inlfux cant handle them
                sid[i] = sid[i].replace("/", "\/")
                sid[i] = sid[i].replace("+", "\+")
                bind_params[f"param_sid_{i}"] = sid[i]

            # Build parameters in query
            section = ' OR "system" = '.join(
                ["$" + s for s in list(bind_params.keys())]
            )
            sidsection = f'("system" = {section})'

        bind_params.update(
            {
                "dstart": datetime_start.isoformat() + "Z",
                "dend": datetime_end.isoformat() + "Z",
            }
        )

        # Prepare query
        if aggregated:
            query = f"""
                SELECT sum("output") AS load, count("output") AS nEntries
                FROM (
                    SELECT mean("output") AS output
                    FROM "realised".."power"
                    WHERE
                        {sidsection} AND
                        time >= $dstart AND
                        time <= $dend
                    GROUP BY time({forecast_resolution.replace("T", "m")}), "system" fill(null)
                )
                WHERE time <= NOW()
                GROUP BY time({forecast_resolution.replace("T", "m")})
            """
        else:
            query = f"""
                SELECT "output" AS load, "system"
                FROM "realised".."power"
                WHERE
                    {sidsection} AND
                    time >= $dstart AND
                    time < $dend fill(null)
            """

        # Query load
        result = _DataInterface.get_instance().exec_influx_query(query, bind_params)

        # no data was found, return empty dataframe
        if "power" not in result:
            return pd.DataFrame()

        result = result["power"]

        if aggregated:
            result = result[["load", "nEntries"]]
        else:
            result = result.pivot_table(index=result.index, columns="system")["load"]
            result = result.resample(forecast_resolution).mean()
            return result

        result = result.dropna()
        if average_output:
            result["load"] = result["load"] / result["nEntries"]

        outputcols = ["load"]
        return result[outputcols]

    def get_load_created_after(
        self, sid: str, created_after: datetime.datetime, group_by_time: str = "5m"
    ) -> pd.DataFrame:
        """Get load created after a certain datetime for a given system id.

        Args:
            sid (str): System id.
            created_after (datetime): Created after datetime.
            group_by_time (str, optional): Group by time. Defaults to "5m".

        Returns:
            pd.DataFrame: Load created after requested datetime.
        """
        # Validate forecast resolution to prevent injections
        self._check_influx_group_by_time_statement(group_by_time)

        bind_params = {"sid": sid}
        query = f"""
            SELECT mean("output") as output, min("created") as created
            FROM "realised".."power"
            WHERE "system" = $sid
            GROUP BY time({group_by_time})
        """

        load = _DataInterface.get_instance().exec_influx_query(query, bind_params)

        return {
            "power": load["power"][load["power"]["created"] > created_after][["output"]]
        }

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

    def get_curtailments(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        name: str,
        resolution: str = "15T",
    ) -> pd.DataFrame:
        """Get curtailments from influx
        input:
            - datetime_start (pd.Datetime)
            - datetime_end (pd.Datetime)
            - name (str): name of curtailment
            - resolution (str): time resolution in pd.resample() format. e.g. 15T

        return
            - pd.DataFrame(index=pd.DatetimeIndex, columns=['curtailment_fraction'])"""

        # Validate forecast resolution to prevent injections
        self._check_influx_group_by_time_statement(resolution.replace("T", "m"))

        bind_params = {
            "name": name,
            "dstart": datetime_start.isoformat(),
            "dend": datetime_end.isoformat(),
        }

        q = f"""
            SELECT mean("curtailment") as curtailment_fraction
            FROM "realised".."curtailments"
            WHERE ("curtailment_name" = $name) AND time >= $dstart and time < $dend'
            GROUP BY time({resolution.replace("T", "m")}) fill(null)
        """

        # Excecute query
        res = _DataInterface.get_instance().exec_influx_query(q, bind_params)
        if len(res) == 0:
            return pd.DataFrame()
        else:
            return res["curtailments"].dropna().tz_localize("UTC")

    def _get_states_from_db(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = "15T",
        flexnet_name: str = "BEMMEL_9017589K_10-1V2LS",
    ) -> pd.DataFrame:

        # Validate forecast resolution to prevent injections
        self._check_influx_group_by_time_statement(
            forecast_resolution.replace("T", "m")
        )

        bind_params = {
            "name": flexnet_name,
            "dstart": datetime_start.isoformat(),
            "dend": datetime_end.isoformat(),
        }

        query = f"""
            SELECT last("output")
            FROM "realised".."power"
            WHERE ("system" = $name AND time >= $dstart and time < $dend')
            GROUP BY time({forecast_resolution.replace("T", "m")}) fill(previous)
        """

        # Excecute query
        res = _DataInterface.get_instance().exec_influx_query(query, bind_params)

        # Check if we got a DataFrame with the column name we expect
        if "power" in res:
            states = pd.DataFrame(res["power"]).rename(columns={"last": "state"})

            return states
        else:
            return pd.DataFrame()

    def get_states_flexnet(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = "15T",
        flexnet_name: str = "BEMMEL_9017589K_10-1V2LS",
    ) -> pd.DataFrame:
        """Get flexnet states for given flexnet name.
        If no result is found, return empty dataframe.

        Parameters:
            - datetime_start: "2018-10-12 08:45:00"
            - datetime_end: "2018-12-12 08:45:00"
            - forecast_resolution = '15T'
            - flexnet_name: name of the flexnet

        Output:
            - pd.DataFrame(index=datetimeIndex, columns)
            columns = [last]
        """

        # make sure inputs are datetime aware datetimes
        datetime_start = pd.to_datetime(datetime_start, utc=True)
        datetime_end = pd.to_datetime(datetime_end, utc=True)

        # Get flexnet states from database
        # Define starting variables
        extended_period = 0
        states = pd.DataFrame()

        # Loop until we have enough information
        while len(states[states.isna().any(axis=1)]) > 0 or len(states) == 0:
            # Get states for requested period
            states = self._get_states_from_db(
                datetime_start - timedelta(days=extended_period),
                datetime_end,
                forecast_resolution=forecast_resolution,
                flexnet_name=flexnet_name,
            )

            # Select the requested period
            states = states[datetime_start:datetime_end]

            # Extend by 90 days
            extended_period = extended_period + 90

            # Safety break to prevent being stuck in an infinte loop
            if extended_period > 900:
                break

        return states

    def get_load_created_datetime_sid(
        self,
        sid: str,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        limit: int,
    ) -> pd.DataFrame:
        """Helper function so the other function can be accurately unit-tested.
        This function gets a dataframe of time, created for a given sid.

        Args:
            sid (str): sid of the desired system
            datetime_start (str): start of period
            datetime_end (str): end of period
            limit (int): maximum number of rows retrieved. Max is 10000.

        Returns:
            pd.DataFrame(index=datetime, columns=[created])
        """
        limit = min(limit, 10000)

        bind_params = {
            "name": sid,
            "dstart": datetime_start.isoformat(),
            "dend": datetime_end.isoformat(),
            "limit": limit,
        }

        q = """
            SELECT created FROM "realised".."power"
            WHERE "system" = $name AND time >= $dstart and time < $dend fill(null)
            LIMIT $limit
        """

        createds = _DataInterface.get_instance().influx_client.query(q, bind_params)[
            "power"
        ]
        return createds

    @staticmethod
    def _check_influx_group_by_time_statement(statement: str) -> None:
        # Validate forecast resolution to prevent injections
        if not re.match(
            r"[0-9]+([unµm]?s|m|h|d|w)",
            statement,
        ):
            raise ValueError("Forecast resolution does not have the allowed format!")
