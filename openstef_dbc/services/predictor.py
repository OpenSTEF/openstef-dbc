# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
import datetime
from enum import Enum
from typing import List, Optional, Tuple, Union

import pandas as pd
from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.services.weather import Weather
from openstef_dbc.utils import (
    genereate_datetime_index,
    process_datetime_range,
    parse_influx_result,
)


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
        country: str = "NL",
        source: Union[List[str], str] = "optimum",
        predictor_groups: Union[List[PredictorGroups], List[str], None] = None,
        entsoe_bidding_zone: str = "NL",
        add_weather_params: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Get predictors.

        Get predictors for a given datetime range. Optionally predictor groups can be
        selected. If the WEATHER_DATA group is included a location is required.

        Args:
            datetime_start (datetime): Date time end.
            datetime_end (datetime): Date time start.
            location (Union[str, Tuple[float, float]], optional): Location (for weather data).
                Defaults to None.
            country (str, optional): Country code (for weather data).
                Defaults to "NL".
            source (str or list of str): which weather models should be used.
                Options: "OWM", "DSN", "WUN", "harmonie", "harm_arome", "harm_arome_fallback", "icon", "optimum",
                Default: 'optimum'. This combines harmonie, harm_arome, icon and DSN,
                taking the (heuristicly) best available source for each moment in time
            predictor_groups (Optional[List[str]], optional): The groups of predictors
                to include (see the PredictorGroups enum for allowed values). When set to
                None or not given all predictor groups will be returned. Defaults to None.
            entsoe_bidding_zone (str, optional): Name of the market place if market data is requested.
                Default to "NL".
            add_weather_params (Optional[List[str]], optional): Additional weather parameters
                / features to include. Defaults to None.

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
        predictors = []

        if PredictorGroups.WEATHER_DATA in predictor_groups:
            weather_data_predictors = self.get_weather_data(
                datetime_start,
                datetime_end,
                location=location,
                country=country,
                forecast_resolution=forecast_resolution,
                source=source,
                add_weather_params=add_weather_params,
            )
            predictors.append(weather_data_predictors)

        if PredictorGroups.MARKET_DATA in predictor_groups:
            market_data_predictors = self.get_market_data(
                datetime_start, datetime_end, forecast_resolution, entsoe_bidding_zone
            )
            predictors.append(market_data_predictors)

        if PredictorGroups.LOAD_PROFILES in predictor_groups:
            load_profiles_predictors = self.get_load_profiles(
                datetime_start, datetime_end, forecast_resolution
            )
            predictors.append(load_profiles_predictors)

        return pd.concat(predictors, axis=1)

    def get_market_data(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = None,
        entsoe_bidding_zone: str = "NL",
    ) -> pd.DataFrame:
        electricity_price = self.get_day_ahead_electricity_price(
            datetime_start, datetime_end, forecast_resolution, entsoe_bidding_zone
        )

        return electricity_price

    def get_day_ahead_electricity_price(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        forecast_resolution: str = None,
        entsoe_bidding_zone: str = "NL",
    ) -> pd.DataFrame:
        bind_params = {
            "_start": datetime_start,
            "_stop": datetime_end,
            "entsoe_bidding_zone": entsoe_bidding_zone,
        }

        query = f"""
            from(bucket: "forecast_latest/autogen")
                |> range(start: {bind_params["_start"].strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {bind_params["_stop"].strftime('%Y-%m-%dT%H:%M:%SZ')}) 
                |> filter(fn: (r) => r._measurement == "marketprices" and r._field == "Price" and r.Name=="{bind_params["entsoe_bidding_zone"]}")
        """

        result = _DataInterface.get_instance().exec_influx_query(query, bind_params)

        # For multiple Fields a list is returned.
        if isinstance(result, list):
            result = pd.concat(result)[["_value", "_field", "_time"]]

        # Check if response is empty
        if not result.empty:
            electricity_price = parse_influx_result(result)
        else:
            return pd.DataFrame(
                index=genereate_datetime_index(
                    start=datetime_start, end=datetime_end, freq=forecast_resolution
                )
            )
        electricity_price.rename(
            columns=dict(Price="day_ahead_electricity_price"), inplace=True
        )

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

            MFFBAS supplies the SJI (Standaard Jaarinvoeding) and SJA (Standaard Jaarafname)
            load profiles for The Netherlands. (These load profiles are the successors of
            the SJV (Standaard Jaarverbruik) profiles from NEDU.) For more information see:
            https://www.mffbas.nl/documenten/

        Returns:
            pandas.DataFrame: TDCV load profiles (if available)

        """
        # select all fields which start with 'sjv'
        # (there is also a 'year_created' tag in this measurement)
        bind_params = {
            "_start": datetime_start,
            "_stop": datetime_end,
        }

        sjv_profles = [
            "E1A_AMI_A",
            "E1A_AMI_I",
            "E1A_AZI_A",
            "E1A_AZI_I",
            "E1B_AMI_A",
            "E1B_AMI_I",
            "E1B_AZI_A",
            "E1B_AZI_I",
            "E1C_AMI_A",
            "E1C_AMI_I",
            "E1C_AZI_A",
            "E1C_AZI_I",
            "E2A_AMI_A",
            "E2A_AMI_I",
            "E2A_AZI_A",
            "E2A_AZI_I",
            "E2B_AMI_A",
            "E2B_AMI_I",
            "E2B_AZI_A",
            "E2B_AZI_I",
            "E3A_A",
            "E3A_I",
            "E3B_A",
            "E3B_I",
            "E3C_A",
            "E3C_I",
            "E3D_A",
            "E3D_I",
            "E4A_A",
            "E4A_I",
        ]

        field_selection = '" or r["_field"] == "'.join(sjv_profles)
        query = f"""
        from(bucket: "realised/autogen")
            |> range(start: {bind_params["_start"].strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {bind_params["_stop"].strftime('%Y-%m-%dT%H:%M:%SZ')}) 
            |> filter(fn: (r) => r["_measurement"] == "sjv")
            |> filter(fn: (r) => r["_field"] == "{field_selection}")
            |> aggregateWindow(every: {forecast_resolution[:-3]}m, fn: mean, createEmpty: false)
            |> yield(name: "mean")
        
        """
        result = _DataInterface.get_instance().exec_influx_query(
            query, bind_params=bind_params
        )
        # For multiple Fields a list is returned.
        if isinstance(result, list):
            result = pd.concat(result)[["_value", "_field", "_time"]]

        if not result.empty:
            load_profiles = parse_influx_result(result)
        else:
            return pd.DataFrame(
                index=genereate_datetime_index(
                    start=datetime_start, end=datetime_end, freq=forecast_resolution
                )
            )

        if forecast_resolution and load_profiles.empty is False:
            load_profiles = load_profiles.resample(forecast_resolution).interpolate(
                limit=3
            )
        return load_profiles.shift(periods=-1, freq=forecast_resolution)

    def get_weather_data(
        self,
        datetime_start: datetime.datetime,
        datetime_end: datetime.datetime,
        location: Union[Tuple[float, float], str],
        forecast_resolution: str = None,
        country: str = "NL",
        source: Union[List[str], str] = "optimum",
        add_weather_params: Optional[List[str]] = None,
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
            *(add_weather_params or []),
        ]
        weather_data = Weather().get_weather_data(
            location,
            weather_params,
            datetime_start,
            datetime_end,
            source=source,
            country=country,
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
