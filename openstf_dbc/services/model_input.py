# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime, timedelta
import pytz

import numpy as np
import pandas as pd

from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.services.weather import Weather
from openstf_dbc.services.systems import Systems
from openstf_dbc.services.ems import Ems
from openstf_dbc.services.predictor import Predictor

# TODO refactor and include in preprocessing and make uniform for making predictions and training models
class ModelInput:
    def get_model_input(
        self,
        pid=295,
        location="Arnhem",
        datetime_start=None,
        datetime_end=None,
        forecast_resolution="15T",
    ):
        """Based on the sid for a transformer this script gets data (with a 15min.
        resolution) of the past 7 days. It gets four kinds of data: the realised
        load, the weather data, the APX energy prices, and the prices of natural gas.

        If the forecast_resolution is finer than the data resolution, the price data
        is filled, while the weather and load data is interpolated.

        Keyword arguments:
        name            --  the name (corresponding to an sid) of the transformers
                            whose data we want to receive and predict (Default is Zvh_V161)
                            Alternatively, the name can contain multiple sid's, split by a '+'
        location        --  (lat, lon) tuple with coordinates.
        datetime_start   --  start date of the data collection in string "YYYY-MM-DD"
                            format (default is two weeks ago)
        datetime_end     --  final date (non-inclusive) of the data collection in string
                            "YYYY-MM-DD" format (default is two days from now)
        forecast_resolution  --  time resolution that the data should have (type is
                            string, default is 15min, format M W D T S)
        """

        # TODO remove location as an argument and get location by pid from the sql database/API

        if datetime_start is None:
            datetime_start = str(datetime.utcnow().date() - timedelta(14))
        if datetime_end is None:
            datetime_end = str(datetime.utcnow().date() + timedelta(3))

        # Get load data
        load = Ems().get_load_pid(
            pid, datetime_start, datetime_end, forecast_resolution
        )
        if len(load) == 0:
            raise Warning("Historic load is empty.")

        # Get APX price data
        apx_data = Predictor().get_apx(datetime_start, datetime_end)

        # Get gas price data
        gas_data = Predictor().get_gas_price(datetime_start, datetime_end)

        # Get SJV data
        sjv_data = Predictor().get_tdcv_load_profiles(datetime_start, datetime_end)

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
        if "source_1" in list(weather_data):
            weather_data["source"] = weather_data.source_1
            weather_data = weather_data.drop("source_1", axis=1)
        if "input_city_1" in list(weather_data):
            del weather_data["input_city_1"]
        else:
            del weather_data["input_city"]
        del weather_data["source"]

        # Combine data
        result = (
            pd.DataFrame(
                index=pd.date_range(
                    start=datetime_start,
                    end=datetime_end,
                    freq=forecast_resolution,
                    tz="UTC",
                )
            )
            .resample(forecast_resolution)
            .ffill()
        )
        result.index.name = "index"

        # Fill return dataframe with all data collected
        if len(load) > 0:
            result = pd.concat(
                [
                    result,
                    load.resample(forecast_resolution).mean().interpolate(limit=3),
                ],
                axis=1,
            )
        else:
            print("Warning: No load data returned!")
            result["load"] = np.nan
        if apx_data is not None:
            result = pd.concat(
                [result, apx_data.resample(forecast_resolution).ffill()], axis=1
            )
        if len(gas_data) > 0:
            result = pd.concat(
                [result, gas_data.resample(forecast_resolution).ffill()], axis=1
            )
        if weather_data is not None:
            result = pd.concat(
                [
                    result,
                    weather_data.resample(forecast_resolution).interpolate(
                        limit=11
                    ),  # 11 as GFS data has data every 3 hours
                ],
                axis=1,
            )
        if sjv_data is not None:
            result = pd.concat(
                [result, sjv_data.resample(forecast_resolution).interpolate(limit=3)],
                axis=1,
            )

        return result

    def get_solar_input(
        self,
        location,
        forecast_horizon,
        forecast_resolution,
        radius=0,
        history=14,
        datetime_start=None,
        sid=None,
    ):
        """This function retrieves the radiation and cloud forecast for the nearest weather location
        and the relevant pvdata from a specific system or region.
        It interpolates these values according to the forecast resolution.
        Parameters:
            - engine: database connection
            - location: lat/lon values [int] or input city [str] of turbine location
            - radius: 'None' when using a specific system, range in kms when using a region
            - history: days of historic weather and pvdata used, default 14
            - forecastHorizon: length of forecast in minutes [int]
            - forecastResolution: time resolution of forecast in minutes [int]
            - datetime_start: datetime of forecast
            - source: preferred weather source as a string, default for wind is DSN
        """
        if datetime_start is None:
            datetime_start = datetime.utcnow()

        systems_service = Systems()

        # sid selection for pvdata
        if radius == 0:
            if sid is None:
                # print("radius is zero and no sid was given, selecting nearest system")
                systems = systems_service.get_systems_near_location(location, freq=5)
                sid = systems.loc[0].sid  # select nearest system
            # print("Using system", sid, "for this prediction.")
        else:
            systems = systems_service.get_systems_near_location(
                location, radius, freq=5
            )
            sid = systems.sid
            # print("Found", len(systems), "systems for this prediction.")

        # Get weather data
        weather_params = ["radiation", "clouds"]
        start = datetime_start + timedelta(days=-history)  # '2017-10-10'
        end = datetime_start + timedelta(minutes=forecast_horizon)

        weather_data = Weather().get_weather_data(
            location, weather_params, start, end, source="optimum"
        )

        # Interpolate weather data to 15 minute values
        weather_data = weather_data.resample(str(forecast_resolution) + "T").asfreq()
        for col in weather_params:
            if col in weather_data:
                weather_data.loc[:, col] = weather_data.loc[:, col].interpolate(
                    "slinear"
                )

        # Get PV_load from influx (end time at start of forecast)
        end = datetime_start
        pvdata = Ems().get_load_sid(
            sid, start, end, "15T", aggregated=True, average_output=radius == 0
        )

        # If no load was found return None
        if pvdata is not None and not pvdata.empty:
            pvdata.rename(columns={"load": "aggregated"}, inplace=True)

            # Make pvdata always positive!
            pvdata["aggregated"] = pvdata["aggregated"].abs()

            # Merge data frames
            pv_data = pvdata.merge(
                weather_data, left_index=True, right_index=True, how="outer"
            )

            # Set NA values of pvdata in the past to 0
            datetime_start = pytz.utc.localize(datetime_start)
            pv_data.loc[
                (pv_data["aggregated"].isnull()) & (pv_data.index < datetime_start),
                "aggregated",
            ] = 0

            return pd.DataFrame(pv_data)

        return pd.DataFrame()

    def get_wind_input(
        self,
        location,
        hub_height,
        forecast_horizon,
        forecast_resolution,
        datetime_start=None,
        source="optimum",
    ):
        """This function retrieves the wind speed forecast for the nearest weather location
        and calculates the wind speed based on the turbine's hub height.
        It interpolates these values according to the forecast resolution.

        Args:
            location: lat/lon values [int] or input city [str] of turbine location
            hubHeight: turbine height in [int]
            forecastHorizon: length of forecast in minutes [int]
            forecastResolution: time resolution of forecast in minutes [int]
            startdatetime: datetime of forecast
            source: preferred weather source as a string, default for wind is DSN
        """

        if datetime_start is None:
            datetime_start = datetime.utcnow()
        datetime_end = datetime_start + timedelta(minutes=forecast_horizon)

        windspeed = Weather().get_weather_data(
            location=location,
            weatherparams="windspeed_100m",
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            source=source,
        )

        # WindSpeedhubheight - not neccesary because windspeed_100m forecast is used
        # forecast_height = 10
        # surface_roughness = 0.143
        # windspeed["windspeedHub"] = windspeed.windspeed * (
        #     (hub_height / forecast_height) ** surface_roughness
        # )

        # interpolate results to 15 minute values
        windspeed = windspeed.resample(str(forecast_resolution) + "T").asfreq()
        windspeed = windspeed.interpolate("cubic")

        return pd.DataFrame(windspeed.windspeed_100m)

    def get_power_curve(self, turbine_type):
        """ "This function retrieves the power curve coefficients from the genericpowercurves table,
        using the turbine type as input."""
        query = "SELECT * FROM genericpowercurves WHERE name = '" + turbine_type + "'"

        result = _DataInterface.get_instance().exec_sql_query(query)

        if result is not None:
            result.rated_power = float(result.rated_power)
            result.slope_center = float(result.slope_center)
            result.steepness = float(result.steepness)

            return result.to_dict(orient="records")[0]
