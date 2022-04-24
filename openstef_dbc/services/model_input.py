# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime, timedelta
from typing import Tuple, Union

import numpy as np
import pandas as pd
import pytz
import structlog
from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.services.ems import Ems
from openstef_dbc.services.predictor import Predictor
from openstef_dbc.services.systems import Systems
from openstef_dbc.services.weather import Weather
from openstef_dbc.utils import process_datetime_range


class ModelInput:
    def __init__(self) -> None:
        self.logger = structlog.get_logger(self.__class__.__name__)

    def get_model_input(
        self,
        pid: int = 295,
        location: Union[Tuple[int, int], str] = "Arnhem",
        datetime_start: str = None,
        datetime_end: str = None,
        forecast_resolution: str = "15min",
    ) -> pd.DataFrame:
        """Get model input.

        Get load and predictors for given pid and datetime range. If the forecast_resolution
        is lower than the data resolution, the price data is filled, while the weather
        and load data is interpolated.

        Args:
            pid (int, optional): Prediction job id. Defaults to 295.
            location (str, optional): Location name or tuple with lat, lon. Defaults to "Arnhem".
            datetime_start (datetime, optional): Start datetime. Defaults to None.
            datetime_end (datetime, optional): End datetime. Defaults to None.
            forecast_resolution (str, optional): Time resolution of model input
                (see pandas Date Offset frequency strings). Defaults to "15min".

        Returns:
            pd.DataFrame: Model input.
        """

        # TODO remove location as an argument and get location by pid from the sql database/API
        # or alternatively use a complete prediction job as input argument
        if datetime_start is None:
            datetime_start = datetime.combine(
                datetime.utcnow().date(), datetime.min.time()
            ) - timedelta(14)

        if datetime_end is None:
            datetime_end = datetime.combine(
                datetime.utcnow().date(), datetime.min.time()
            ) + timedelta(3)

        # Process datetimes (rounded, timezone, frequency) and generate index
        datetime_start, datetime_end, datetime_index = process_datetime_range(
            start=datetime_start,
            end=datetime_end,
            freq=forecast_resolution,
        )

        # Get load
        load = Ems().get_load_pid(
            pid, datetime_start, datetime_end, forecast_resolution
        )

        # Get predictors
        predictors = Predictor().get_predictors(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            forecast_resolution=forecast_resolution,
            location=location,
        )

        # Create model input with datetime index
        model_input = pd.DataFrame(index=datetime_index)
        model_input.index.name = "index"

        # Add load if available, else add nan column
        if not load.empty:
            load = load.resample(forecast_resolution).mean().interpolate(limit=3)
            model_input = pd.concat([model_input, load], axis=1)
        else:
            self.logger.warning("No load data returned, fill with NaN.")
            model_input["load"] = np.nan
        # Add predictors
        model_input = pd.concat([model_input, predictors], axis=1)

        return model_input

    def get_solar_input(
        self,
        location: Union[Tuple[float, float], str],
        forecast_horizon: int,
        forecast_resolution: int,
        radius: float = 0.0,
        history: int = 14,
        datetime_start: datetime = None,
        sid: str = None,
    ) -> pd.DataFrame:
        """This function retrieves the radiation and cloud forecast for the nearest weather location
        and the relevant pvdata from a specific system or region.
        It interpolates these values according to the forecast resolution.
        Parameters:
            - engine: database connection
            - location: lat/lon values [float] or input city [str] of turbine location
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
                self.logger.debug(
                    "Radius is zero and no sid was given, selecting nearest system"
                )
                systems = systems_service.get_systems_near_location(location, freq=5)
                sid = systems.loc[0].sid  # select nearest system
                self.logger.debug("Nearest system is {sid}")
        else:
            systems = systems_service.get_systems_near_location(
                location, radius, freq=5
            )
            sid = systems.sid
            self.logger.debug(f"Found {len(systems)} systems near this location")

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
        location: Union[Tuple[float, float], str],
        hub_height: int,
        forecast_horizon: int,
        forecast_resolution: int,
        datetime_start: datetime = None,
        source: str = "optimum",
    ) -> pd.DataFrame:
        """This function retrieves the wind speed forecast for the nearest weather location
        and calculates the wind speed based on the turbine's hub height.
        It interpolates these values according to the forecast resolution.

        Args:
            location: lat/lon values [int] or input city [str] of turbine location
            forecast_horizon: length of forecast in minutes [int]
            forecast_resolution: time resolution of forecast in minutes [int]
            datetime_start: datetime of forecast
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

        # interpolate results to 15 minute values
        windspeed = windspeed.resample(str(forecast_resolution) + "T").asfreq()
        windspeed = windspeed.interpolate("cubic")

        return pd.DataFrame(windspeed.windspeed_100m)

    @classmethod
    def get_power_curve(cls, turbine_type: str) -> dict:
        """ "This function retrieves the power curve coefficients from the genericpowercurves table,
        using the turbine type as input."""
        bind_params = {"turbine_type": turbine_type}
        query = "SELECT * FROM genericpowercurves WHERE name = %(turbine_type)s"

        result = _DataInterface.get_instance().exec_sql_query(query, bind_params)

        if result is not None:
            result.rated_power = float(result.rated_power)
            result.slope_center = float(result.slope_center)
            result.steepness = float(result.steepness)

            return result.to_dict("records")[0]
