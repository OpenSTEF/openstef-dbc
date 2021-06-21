# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from openstf_dbc.data_interface import _DataInterface


class Systems:
    def get_systems_near_location(
        self, location, radius=15, quality=0.75, freq=None, lag_systems=None
    ):
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
        query = """
            SELECT `sid`, `lat`, `lon`,`region`, ( 6371 * acos( cos( radians({lat}) ) \
        * cos( radians( lat ) ) * cos( radians( lon ) - radians({lon}) ) + sin( radians({lat}) ) \
        * sin( radians( lat ) ) ) ) AS 'distance' \
        FROM `systems`
        WHERE `qual` > '{quality}'
        """.format(
            lat=str(location[0]), lon=str(location[1]), quality=quality
        )

        # Extend query
        if freq is not None:
            query += """ AND `freq` <= '""" + str(freq) + """'"""
        if lag_systems is not None:
            query += """ AND `lagSystems` <= '""" + str(quality) + """'"""

        # Limit radius to given input radius
        query += (
            """ HAVING `distance` < '""" + str(radius) + """' ORDER BY `distance`;"""
        )

        result = _DataInterface.get_instance().exec_sql_query(query)
        return result

    def get_systems_by_pid(self, pid, return_list=False):
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

        return systems.to_dict(orient="records")

    def get_pv_systems_with_incorrect_location(self):
        """Get PV systems with missing (or incorrect) lat/lon"""
        query = """
            SELECT * FROM `systems`
            WHERE `sid` LIKE 'pv_%' AND (`lat` IS NULL OR `lon` IS NULL)
        """
        return _DataInterface.get_instance().exec_sql_query(query)

    def get_random_pv_systems(self, autoupdate=1, limit=None):
        limit_query = ""

        if limit is not None:
            limit_query = f"LIMIT {limit}"

        query = f"""
            SELECT sid, qual, freq, lag
            FROM systems
            WHERE left(sid, 3) = 'pv_' AND autoupdate = {autoupdate}
            ORDER BY RAND() {limit_query}
        """

        return _DataInterface.get_instance().exec_sql_query(query)
