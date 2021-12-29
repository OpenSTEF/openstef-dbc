# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
import datetime
from enum import Enum
from typing import List, Optional, Tuple, Union

import pandas as pd
from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.services.weather import Weather
from openstef_dbc.utils import genereate_datetime_index, process_datetime_range


class PredictorGroups(Enum):
    MARKET_DATA = "market_data"
    WEATHER_DATA = "weather_data"
    LOAD_PROFILES = "load_profiles"


class Predictor:
    def get_predictors(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: Optional[str] = None,
        location: Union[str, Tuple[float, float]] = None,
        predictor_groups: Union[List[PredictorGroups], List[str], None] = None,
    ) -> pd.DataFrame:
        """Get predictors.

        Get predictors for a given datetime range. Optionally predictor groups can be
        selected. If the WEATHER_DATA group is included a location is required.

        Args:
            datetime_start (datetime): Date time end.
            datetime_end (datetime): Date time start.
            location (Union[str, Tuple[float, float]], optional): Location (for weather data).
                Defaults to None.
            predictor_groups (Optional[List[str]], optional): The groups of predictors
                to include (see the PredictorGroups enum for allowed values). When set to
                None or not given all predictor groups will be returned. Defaults to None.

        Returns:
            pd.DataFrame: Requested predictors with timezone aware datetime index.
        """

        # Process datetimes (rounded, timezone, frequency) and generate index
        datetime_start, datetime_end, datetime_index = process_datetime_range(
            start=datetime_start,
            end=datetime_end,
            freq=forecast_resolution,
        )
        if predictor_groups is None:
            predictor_groups = [p for p in PredictorGroups]

        # convert strings to enums if required
        predictor_groups = [PredictorGroups(p) for p in predictor_groups]

        if PredictorGroups.WEATHER_DATA in predictor_groups and location is None:
            raise ValueError(
                "Need to provide a location when weather data predictors are requested."
            )
        predictors = pd.DataFrame(index=datetime_index)

        if PredictorGroups.WEATHER_DATA in predictor_groups:
            weather_data_predictors = self.get_weather_data(
                datetime_start,
                datetime_end,
                location=location,
                forecast_resolution=forecast_resolution,
            )
            predictors = pd.concat([predictors, weather_data_predictors], axis=1)

        if PredictorGroups.MARKET_DATA in predictor_groups:
            market_data_predictors = self.get_market_data(
                datetime_start, datetime_end, forecast_resolution=forecast_resolution
            )
            predictors = pd.concat([predictors, market_data_predictors], axis=1)

        if PredictorGroups.LOAD_PROFILES in predictor_groups:
            load_profiles_predictors = self.get_load_profiles(
                datetime_start, datetime_end, forecast_resolution=forecast_resolution
            )
            predictors = pd.concat([predictors, load_profiles_predictors], axis=1)

        return predictors

    def get_market_data(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = None,
    ) -> pd.DataFrame:
        electricity_price = self.get_electricity_price(
            datetime_start, datetime_end, forecast_resolution
        )

        return electricity_price

    def get_electricity_price(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = None,
    ) -> pd.DataFrame:
        database = "forecast_latest"
        measurement = "marketprices"
        bind_params = {
            "dstart": datetime_start.isoformat(),
            "dend": datetime_end.isoformat(),
        }
        query = f"""
            SELECT
                "Price" FROM "{database}".."{measurement}"
            WHERE
                "Name" = 'APX' AND
                time >= $dstart AND
                time <= $dend
        """
        electricity_price = _DataInterface.get_instance().exec_influx_query(
            query, bind_params
        )

        if not electricity_price:
            return pd.DataFrame(
                index=genereate_datetime_index(
                    start=datetime_start, end=datetime_end, freq=forecast_resolution
                )
            )

        electricity_price = electricity_price["marketprices"]

        electricity_price.rename(columns=dict(Price="APX"), inplace=True)

        if forecast_resolution and electricity_price.empty is False:
            electricity_price = electricity_price.resample(forecast_resolution).ffill()

        return electricity_price

    def get_load_profiles(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = None,
    ) -> pd.DataFrame:
        """Get load profiles.

            Get the TDCV (Typical Domestic Consumption Values) load profiles from the
            database for a given range.

            NEDU supplies the SJV (Standaard Jaarverbruik) load profiles for
            The Netherlands. For more information see:
            https://www.nedu.nl/documenten/verbruiksprofielen/

        Returns:
            pandas.DataFrame: TDCV load profiles (if available)

        """
        # select all fields which start with 'sjv'
        # (there is also a 'year_created' tag in this measurement)
        bind_params = {
            "dstart": datetime_start.isoformat(),
            "dend": datetime_end.isoformat(),
        }

        query = f"""
            SELECT
                /^sjv/ FROM "realised".."sjv"
            WHERE
                time >= $dstart AND
                time <= $dend
        """
        load_profiles = _DataInterface.get_instance().exec_influx_query(
            query, bind_params=bind_params
        )

        if not load_profiles:
            return pd.DataFrame(
                index=genereate_datetime_index(
                    start=datetime_start, end=datetime_end, freq=forecast_resolution
                )
            )

        load_profiles = load_profiles["sjv"]

        if forecast_resolution and load_profiles.empty is False:
            load_profiles = load_profiles.resample(forecast_resolution).interpolate(
                limit=3
            )
        return load_profiles

    def get_weather_data(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        location: Union[Tuple[float, float], str],
        forecast_resolution: str = None,
    ) -> pd.DataFrame:
        # Get weather data
        weather_params = [
            "clouds",
            "radiation",
            "temp",
            "winddeg",
            "windspeed",
            "windspeed_100m",
            "pressure",
            "humidity",
            "rain",
            "mxlD",
            "snowDepth",
            "clearSky_ulf",
            "clearSky_dlf",
            "ssrunoff",
        ]
        weather_data = Weather().get_weather_data(
            location,
            weather_params,
            datetime_start,
            datetime_end,
            source="optimum",
        )

        # Post process weather data
        # This might not be required anymore?
        if "source_1" in list(weather_data):
            weather_data["source"] = weather_data.source_1
            weather_data = weather_data.drop("source_1", axis=1)

        if "source" in list(weather_data):
            del weather_data["source"]

        if "input_city_1" in list(weather_data):
            del weather_data["input_city_1"]
        elif "input_city" in list(weather_data):
            del weather_data["input_city"]

        if forecast_resolution and weather_data.empty is False:
            weather_data = weather_data.resample(forecast_resolution).interpolate(
                limit=11
            )  # 11 as GFS data has data every 3 hours

        return weather_data
