# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
from typing import Union, Tuple, List

import pandas as pd

from openstef_dbc.data_interface import _DataInterface


class Systems:
    def get_systems_near_location(
        self,
        location: Union[Tuple[float, float], str],
        radius: float = 15.0,
        quality: float = 0.75,
        freq: int = None,
        lag_systems: float = None,
    ) -> pd.DataFrame:
        """Retrieve all systems near a given location.
        Only return systems which exceed the quality, freq, lag_systems requirements.

        Parameters:
            - location: (lat, lon)
            - radius: float (km)
            - quality: float
            - freq: int (minutes)
            - lag_systems: float (minutes)

        Return:
            - pd.DataFrame(sid, lat, lon, region, distance)
        """

        # Define base query
        # use Great-circle calculation to determine systems within radius (6371 is radius of Earth)
        bind_params = {
            "lat": str(location[0]),
            "lon": str(location[1]),
            "quality": quality,
            "radius": str(radius),
        }
        query = """
                    SELECT `sid`, `lat`, `lon`,`region`, ( 6371 * acos( cos( radians(%(lat)s) ) \
                * cos( radians( lat ) ) * cos( radians( lon ) - radians(%(lon)s) ) + sin( radians(%(lat)s) ) \
                * sin( radians( lat ) ) ) ) AS 'distance' \
                FROM `systems`
                WHERE `qual` > '%(quality)s'
                """

        # Extend query
        if freq is not None:
            bind_params["freq"] = str(freq)
            query += """ AND `freq` <= %(freq)s"""
        if lag_systems is not None:
            query += """ AND `lagSystems` <= %(quality)s"""

        # Limit radius to given input radius
        query += """ HAVING `distance` < %(radius)s ORDER BY `distance`;"""

        result = _DataInterface.get_instance().exec_sql_query(query, bind_params)
        return result

    def get_systems_by_pid(
        self, pid: int, return_list: bool = False
    ) -> Union[pd.DataFrame, List[dict]]:
        """Get all prediction job systems for a given prediction job id.

            The `systems` table will be joint with the `predicions_systems` table
            in order to return all columns of `systems` and `predictions_systems`.
            The latter will add the `factor` column. The factor determines wether or
            not a systems should be added or subtracted for this pid.

        Args:
            pid (Union[int, str): Prediction job id
            return_list (Optional[bool]): Return a List[dict] of prediction job systems.
                Defaults to False.
        Returns:
            Union[pandas.DataFrame, List[dict]]: Prediction job systems
        """

        query = """
        SELECT * from systems
        INNER JOIN predictions_systems
            ON predictions_systems.system_id=systems.sid
        WHERE predictions_systems.prediction_id=%(pid)s
        """

        systems = _DataInterface.get_instance().exec_sql_query(
            query, params={"pid": pid}
        )

        if return_list is False:
            return systems

        return systems.to_dict("records")

    def get_pv_systems_with_incorrect_location(self) -> pd.DataFrame:
        """Get PV systems with missing (or incorrect) lat/lon"""
        query = """
            SELECT * FROM `systems`
            WHERE `sid` LIKE 'pv_%' AND (`lat` IS NULL OR `lon` IS NULL)
        """
        return _DataInterface.get_instance().exec_sql_query(query)

    def get_random_pv_systems(
        self, autoupdate: int = 1, limit: int = None
    ) -> pd.DataFrame:
        limit_query = ""

        bind_params = {"autoupdate": autoupdate}

        if limit is not None:
            bind_params["limit"] = limit
            limit_query = f"LIMIT %(limit)s"

        query = f"""
            SELECT sid, qual, freq, lag
            FROM systems
            WHERE left(sid, 3) = 'pv_' AND autoupdate = %(autoupdate)s
            ORDER BY RAND() {limit_query}
        """

        return _DataInterface.get_instance().exec_sql_query(query, bind_params)

    def get_api_key_for_system(self, sid: str) -> str:
        """Get (sysetm) API key that is connected to a given system.

        Args:
            sid (str): The system ID

        Returns:
            str: The API key that is connected to the given system.
        """
        bind_params = {"system": sid}

        query = """
            SELECT 
                sa.apiKey 
            FROM `systems` as s 
            LEFT JOIN `systemsApiKeys` as sa ON s.measurements_customer_api_key_id = sa.id 
            WHERE s.sid = %(system)s;
        """

        result = _DataInterface.get_instance().exec_sql_query(query, bind_params)
        if isinstance(result, pd.DataFrame) and result.empty:
            return ""
        else:
            return result["apiKey"][0]

    def get_api_keys_for_systems(self, sids: list[str]) -> list[str]:
        """Get (sysetm) API keys that are connected to a given list of systems.

        Args:
            sid (str): The list with system IDs

        Returns:
            list(str): The API keys that are connected to the given systems in the list.
        """
        api_keys = []
        for sid in sids:
            api_keys.append(self.get_api_key_for_system(sid))
        return list(set(api_keys))
