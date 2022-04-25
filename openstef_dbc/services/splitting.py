# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from typing import Union, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.services.weather import Weather
from openstef_dbc.services.systems import Systems
from openstef_dbc.services.ems import Ems
from openstef_dbc.services.predictor import Predictor


# TODO check if this functionality should be transfered to a data preparation step for energy splitting in ktprognoses
class Splitting:
    def get_energy_split_coefs(self, pj: dict, mean: bool = False) -> dict:
        """
            Method to retrieve latest energy splitting coefficients from the database.
            If mean is passed as True the average values are given for the last 180 days.
        Args:
            pj: (dict) Prediction job
            mean: (bool), if true the average of splitting coefficients in the last 180 days are returned.

        Returns:
            (dict) with latest splitting coefficients, or average vales of the last 180 days are returned

        """

        # Retrieve the average values of the coefficients of the last 180 if requested
        if mean:
            start_date = datetime.utcnow() - timedelta(days=180)
            bind_params = {"pid": pj["id"], "dstart": start_date.isoformat()}
            query = (
                "SELECT ec.coef_name, AVG(ec.coef_value) FROM energy_split_coefs as ec "
                "WHERE ec.pid = %(pid)s AND ec.created > %(dstart)s GROUP BY ec.coef_name "
            )
        # Retrieve latest coefficients otherwise
        else:
            bind_params = {"pid": pj["id"]}
            query = (
                "SELECT ec.coef_name,ec.coef_value FROM energy_split_coefs as ec WHERE  ec.pid = %(pid)s "
                "AND ec.created = (SELECT max(energy_split_coefs.created) from energy_split_coefs "
                "WHERE energy_split_coefs.pid = %(pid)s)"
            )
        # Execute query
        result = _DataInterface.get_instance().exec_sql_query(query, bind_params)

        # Make output dict
        if result is not None:
            result = result.set_index("coef_name")
            if mean:
                result = result.to_dict()["AVG(ec.coef_value)"]
            else:
                result = result.to_dict()["coef_value"]
        else:
            result = {}

        return result

    def get_wind_ref(
        self,
        location: Union[Tuple[float, float], str],
        datetime_start: datetime,
        datetime_end: datetime,
    ) -> pd.DataFrame:
        """Function that gets windspeed data from the influx database and converts it to windref data suitable
        for splitting energy.
        Parameters:
            location: str, country or location
            datetime_start: datetime, start time of required windref
            datetime_end: datetime, end time of required windref
        Output:
            windref: pandas dataframe containing the windref data"""

        # Get weather information from the influx database
        wind_speed = Weather().get_weather_data(
            location,
            ["windspeed_100m"],
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            source="optimum",
        )

        wind_ref = self._calculate_windspeed_at_hubheight(
            self, windspeed=wind_speed["windspeed_100m"], fromheight=100, hub_height=100
        )
        wind_ref = wind_ref / np.abs(np.amax(wind_ref))

        result = pd.DataFrame()
        result["windspeed"] = wind_ref

        # Return result
        return result

    def _get_solar_ref(
        self,
        location: Union[Tuple[float, float], str],
        datetime_start: datetime = None,
        datetime_end: datetime = None,
    ) -> pd.DataFrame:
        """Function that gets PV data from the influx database and converts it to solar_ref data suitable
        for splitting energy.
        Parameters:
            location: list, list of lat and lon of desired location
            datetime_start: datetime, start time of required solar_ref
            datetime_end: datetime, end time of required solar_ref
        Output:
            solar_ref: pandas dataframe containing the solar_ref data"""

        if datetime_start is None:
            datetime_start = datetime.utcnow().date() - timedelta(14)
        if datetime_end is None:
            datetime_end = datetime.utcnow().date() + timedelta(3)

        # Look for PV systems near desired location
        systems = Systems().get_systems_near_location(location, quality=0.95, freq=5)

        if len(systems) == 0:
            systems = Systems().get_systems_near_location(
                location, radius=30, quality=0.95, freq=5
            )

        # Get PV data from the archive table
        pvdata = Ems().get_load_sid(
            sid=systems["sid"],
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            forecast_resolution="15T",
            aggregated=True,
            average_output=True,
        )
        pvdata.rename(columns={"load": "aggregated"}, inplace=True)

        # Normalize
        pvdata.aggregated = pvdata.aggregated / np.percentile(pvdata.aggregated, 99.0)

        # Return results
        return pvdata[["aggregated"]]

    def get_input_energy_splitting(
        self,
        pj: dict,
        datetime_start: datetime = None,
        datetime_end: datetime = None,
        ignore_factor: bool = False,
    ) -> pd.DataFrame:
        if datetime_start is None:
            datetime_start = datetime.utcnow() - timedelta(days=90)
        if datetime_end is None:
            datetime_end = datetime.utcnow()

        # Get standard load profiles (StandaardJaarVerbruik in Dutch)
        sjv = Predictor().get_load_profiles(datetime_start, datetime_end)

        # Get windpower reference
        wind_ref = self.get_wind_ref(
            (pj["lat"], pj["lon"]), datetime_start, datetime_end
        )

        # Get load data
        load = Ems().get_load_pid(
            pj["id"],
            datetime_start,
            datetime_end,
            ignore_factor=ignore_factor,
        )

        # Get solar (PV) power reference
        solar_ref = self._get_solar_ref(
            location=(pj["lat"], pj["lon"]),
            datetime_start=datetime_start,
            datetime_end=datetime_end,
        )

        # Resample to 15min
        solar_ref = solar_ref.resample("15T").mean()

        # Invert solar_ref and windref as electricity is produced and not consumed
        solar_ref *= -1
        wind_ref *= -1

        # Merge solar_ref and windref
        input_split_function = load.merge(
            wind_ref, left_index=True, right_index=True, how="outer"
        )
        input_split_function = input_split_function.merge(
            solar_ref, left_index=True, right_index=True, how="outer"
        )
        input_split_function = input_split_function.merge(
            sjv, left_index=True, right_index=True, how="outer"
        )

        # Drop rows with duplicate indices
        input_split_function = input_split_function[
            ~input_split_function.index.duplicated()
        ]

        # Rename columns to match their contents and the requirements of FindComponents()
        input_split_function.rename(columns={"windspeed": "wind_ref"}, inplace=True)
        input_split_function.rename(columns={"aggregated": "pv_ref"}, inplace=True)

        # Replace infs with nans and drop all rows with Nans
        input_split_function.replace([np.inf, -np.inf], np.nan)
        input_split_function.dropna(inplace=True)

        return input_split_function

    @staticmethod
    def _calculate_windspeed_at_hubheight(
        self,
        windspeed: Union[float, pd.Series],
        fromheight: float = 10.0,
        hub_height: float = 100.0,
    ) -> pd.Series:
        """
        function that extrapolates a wind from a certain height to 100m
        According to the wind power law (https://en.wikipedia.org/wiki/Wind_profile_power_law)

        input:
            - windspeed: float OR pandas series of windspeed at height = height
            - fromheight: height (m) of the windspeed data. Default is 10m
            - hubheight: height (m) of the turbine
        returns:
            - the windspeed at hubheight."""
        alpha = 0.143

        if not isinstance(windspeed, (np.ndarray, float, int, pd.Series)):
            raise TypeError(
                "The windspeed is not of the expected type!\n\
                            Got {}, expected np.ndarray, pd series or numeric".format(
                    type(windspeed)
                )
            )

        try:
            if any(windspeed < 0):
                raise ValueError(
                    "The windspeed cannot be negative, as it is the lenght of a vector"
                )
        except TypeError:
            if windspeed < 0:
                raise ValueError(
                    "The windspeed cannot be negative, as it is the lenght of a vector"
                )
            windspeed = abs(windspeed)

        return windspeed * (hub_height / fromheight) ** alpha
