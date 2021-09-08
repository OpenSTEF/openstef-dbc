# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from enum import Enum
from typing import List, Optional, Tuple, Union

import pandas as pd
from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.services.weather import Weather


class PredictorGroups(Enum):
    MARKET_DATA = "market_data"
    WEATHER_DATA = "weather_data"
    LOAD_PROFILES = "LOAD_PROFILES"


class Predictor:
    def get_predictors(
        self,
        datetime_start,
        datetime_end,
        location: Union[str, Tuple[float, float]] = None,
        predictor_groups: Optional[List[str]] = None,
    ):
        """Get predictors.

        Get predictors for a given datetime range. Optionally predictor groups can be
        selected. If the WEATHER_DATA group is included a location is required.

        Args:
            location (Union[str, Tuple[float, float]], optional): Location (for weather data).
                Defaults to None.
            predictor_groups (Optional[List[str]], optional): The groups of predictors
                to include. Defaults to None.

        Returns:
            pd.DataFrame: Requested predictors.
        """
        if predictor_groups is None:
            predictor_groups = [p for p in PredictorGroups]

        if PredictorGroups.WEATHER_DATA in predictor_groups and location is None:
            raise ValueError(
                "Need to provide a location when weather data predictors are requested."
            )

        predictors = pd.DataFrame()

        if PredictorGroups.WEATHER_DATA in predictor_groups:
            predictors.concat(Weather().get_weather_data(location=location))

        if PredictorGroups.MARKET_DATA in predictor_groups:
            predictors.concat(self.get_market_data(datetime_start, datetime_end))

        if PredictorGroups.LOAD_PROFILES in predictor_groups:
            predictors.concat(self.get_load_profiles(datetime_start, datetime_end))

        return predictors

    def get_market_data(self, datetime_start, datetime_end):
        electricity_price = self.get_electricity_price(datetime_start, datetime_end)
        gas_price = self.get_gas_price(datetime_start, datetime_end)
        return pd.concat(electricity_price, gas_price)

    def get_electricity_price(self, datetime_start, datetime_end):
        query = 'SELECT "Price" FROM "forecast_latest".."marketprices" \
        WHERE "Name" = \'APX\' AND time >= \'{}\' AND time <= \'{}\''.format(
            datetime_start, datetime_end
        )

        result = _DataInterface.get_instance().exec_influx_query(query)

        if result:
            result = result["marketprices"]
            result.rename(columns=dict(Price="APX"), inplace=True)
            return result

    def get_gas_price(self, datetime_start, datetime_end):
        query = "SELECT datetime, price FROM marketprices WHERE name = 'gasPrice' \
                    AND datetime BETWEEN '{start}' AND '{end}' ORDER BY datetime asc".format(
            start=str(datetime_start), end=str(datetime_end)
        )

        result = _DataInterface.get_instance().exec_sql_query(query)
        result.rename(columns={"price": "Elba"}, inplace=True)

        return result

    def get_load_profiles(self, datetime_start, datetime_end):
        """Get load profiles.

            Get the TDCV (Typical Domestic Consumption Values) load profiles from the
            database for a given range.

            NEDU supplies the SJV (Standaard Jaarverbruik) load profiles for
            The Netherlands. For more information see:
            https://www.nedu.nl/documenten/verbruiksprofielen/

        Returns:
            pandas.DataFrame or None: TDCV load profiles (if available)

        """
        # select all fields which start with 'sjv'
        # (there is also a 'year_created' tag in this measurement)
        database = "realised"
        measurement = "sjv"
        query = f"""
            SELECT /^sjv/ FROM "{database}".."{measurement}"
            WHERE time >= '{datetime_start}' AND time <= '{datetime_end}'
        """

        result = _DataInterface.get_instance().exec_influx_query(query)

        if result:
            return result[measurement]
