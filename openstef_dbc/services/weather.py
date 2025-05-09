# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0
from typing import Union, Tuple, List
from datetime import datetime, timedelta

import geopy
import geopy.distance
import numpy as np
import pandas as pd
import pytz
import structlog
import warnings
from influxdb_client.client.warnings import MissingPivotFunction

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.services.write import Write
from openstef_dbc.settings import Settings
from openstef_dbc.utils import genereate_datetime_index, parse_influx_result

warnings.simplefilter("ignore", MissingPivotFunction)


class Weather:
    def __init__(self) -> None:
        self.logger = structlog.get_logger(self.__class__.__name__)

    def get_weather_forecast_locations(
        self, country: str = "NL", active: int = 1
    ) -> List[dict]:
        """Get weather forecast locations.

        Returns:
            List[dict]: List of locations with keys:
                ["city"]:       City of the weather forecast.
                ["lat"]:        Lattitude coordinate.
                ["lon"]:        Longitude coordinate.
                ["country"]:    Country code (2-letter: ISO 3166-1)
        """

        bind_params = {"country": country, "active": active}

        query = """
            SELECT input_city as city, lat, lon, country
            FROM weatherforecastlocations
            WHERE country = :country AND active = :active
        """
        result = _DataInterface.get_instance().exec_sql_query(query, bind_params)

        locations = result.to_dict("records")

        return locations

    def _get_nearest_weather_locations(
        self,
        location: Union[Tuple[float, float], str],
        country: str = "NL",
        threshold: float = 150.0,
        number_locations: int = 1,
    ) -> pd.Series:
        """Find the nearest weather forecast locations.

        Function that, given an location, finds the nearest locations for which a
        weatherforecast is available. A warning is generated when the distance is
        greater than a certain distance (threshold).

        If multiple weather locations have the same distance, only one is returned.

        Args:
            location (str, tuple): Name of the location/city or coordinates (lat, lon).
            country (str): Country code (2-letter: ISO 3166-1).
            threshold (int): Maximum distance [km] before a warning is generated.
            number_locations (int): number of weather locations desired

        Returns:
            Pd.Series: Name of the weather location(s) (distance as index)
        """
        # Get all available cities
        weather_locations = self.get_weather_forecast_locations(
            country=country, active=1
        )

        # If location is string, convert to (lat, lon)
        if type(location) is str:
            location_coordinates = self._get_coordinates_of_location(location)
        else:
            location_coordinates = location

        # Now we have the coordinates of the input_city. Next, find nearest weather location
        distance_dfs = []
        for weather_location in weather_locations:
            coordinates = (weather_location["lat"], weather_location["lon"])

            if np.nan in coordinates:
                raise ValueError(
                    "No coordinates found, pleas make sure a prope location "
                    "is configured in the mySQL database"
                )

            distance = round(
                geopy.distance.geodesic(coordinates, location_coordinates).km, 1
            )
            city = weather_location["city"]
            distance_df = pd.DataFrame([{"distance": distance, "input_city": city}])
            distance_dfs.append(distance_df)

        distances = pd.concat(distance_dfs).set_index("distance")

        nearest_location = distances["input_city"].sort_index().iloc[0:number_locations]

        # Find closest weather location
        if distances.index.min() < threshold:
            return nearest_location

        raise Warning(
            "Closest weather location is farther than threshold: {dist} > {threshold}".format(
                dist=str(min(distances)), threshold=str(threshold)
            )
        )

    def _get_coordinates_of_location(self, location_name: str) -> Tuple[float, float]:
        """Get lat, lon coordinates of location.

        The function tries to get the coordinates from our own sql database,
        if the name is not yet present, Nominatim() is used to retrieve
        lat lons. It then proceeds to add the values to database.

        Args:
            input_city(str): Name of the location/city.

        Returns:
            (tuple): Coordinated:
                [0]: Lattitude
                [1]: Longitude
        """

        # Query corresponding (lat, lon) from SQL database
        binding_params = {"city": location_name}
        query = "SELECT lat, lon from NameToLatLon where regionInput = :city"
        location = _DataInterface.get_instance().exec_sql_query(query, binding_params)

        # If not found
        if len(location) == 0:
            # Get (lat, lon) of location (via google)
            location = geopy.geocoders.Nominatim().geocode(location_name)
            location = (location.latitude, location.longitude)

            # Write found location to MySQL database
            # NOTE Not sure if we should want this. perhaps better to fail and do
            # manual write
            Write().write_location(location_name, location)
        else:
            location = (location["lat"][0], location["lon"][0])

        return location

    def _get_source_run(
        self, forecast_datetime: pd.Series, tAhead: pd.Series
    ) -> pd.Series:
        """Compute the datetime when weather forecast was created

        Args:
            forecast_datetime (pd.Series[datetime]): forecasted datetime.
            tAhead (pd.Series[(int, float)]: forecasting horizon in hours

        Retuns :
            pd.Series of new datetimes
        """

        if not pd.api.types.is_datetime64_any_dtype(forecast_datetime):
            raise ValueError("forecast_datetime must be a Series of datetime.")

        if not pd.api.types.is_numeric_dtype(tAhead):
            raise ValueError("tahead must be a Series of int or float.")

        if len(forecast_datetime) != len(tAhead):
            raise ValueError("forecast_datetime and tAhead must have the same length.")

        # Compute new datetimes
        return forecast_datetime - pd.to_timedelta(tAhead, unit="h")

    def _combine_weather_sources(
        self, result: pd.DataFrame, source_order: List = None
    ) -> pd.DataFrame:
        """
        Function that complete's the weatherdata from the influx database retrieved using get_weatherdata
        Additionally, weatherdata from several sources is combined to a single forecast.
        The preferred source_order is given as argument.

        Args:
            result (pd.DataFrame): return value from the function 'get_weatherdata'
            source_order (list): which weather models should be used (first).
                Options: "OWM", "DSN", "WUN", "harmonie", "harm_arome", "harm_arome_fallback", "optimum", "icon"
                Default: '"harm_arome", "harm_arome_fallback", "harmonie", "icon", "DSN"'.
                This combines harmonie, harm_arome, harm_arome_fallback, ICON and DSN,
                taking the (heuristicly) best available source for each moment in time

        Returns:
            pd.DataFrame: The most recent weather prediction of several sources combined into one dataframe

        Example:
            client = influxdb.DataFrameClient()
            df = QueryWeatherData(client, location='Volkel',
                                  weatherParams=["clouds", "radiation"], source='optimum')
            print(df.head())
        """
        # Combine the multiple sources in the optimal way
        ####
        # step 1: Create list of multiple dataframes,
        # check which of the 'optimum' sources are actually in the list
        if source_order is None:
            source_order = Settings.optimum_weather_sources

        active_sources = [s for s in source_order if s in set(result.source)]
        # for each source a seperate dataframe
        df_list = [result[result.source == s] for s in active_sources]

        # Step two, combine the individual dataframe,
        # keeping data from the most-preferred datasource
        complete_df = df_list.pop(0)
        while len(df_list) > 0:
            df_new_source = df_list.pop(0)
            complete_df = complete_df.combine_first(df_new_source)

        return complete_df

    def get_weather_data(
        self,
        location: Union[Tuple[float, float], str],
        weatherparams: List[str],
        datetime_start: datetime = datetime.utcnow() - timedelta(days=14),
        datetime_end: datetime = datetime.utcnow() + timedelta(days=2),
        source: Union[List[str], str] = "optimum",
        resolution: str = "15min",
        country: str = "NL",
        number_locations: int = 1,
        type: str = "smallest_tAhead",
    ) -> pd.DataFrame:
        """Get weather data from database.

        Additionally, weatherdata from several sources is combined to a single forecast.
        When the source equals "optimum", data from harmonie is preferred,
            completed by data from harm_arome and DSN

        Args:
            location (str): Location. Should be in the stored locations. e.g. Volkel
            weatherparams  (str or list of str): weatherparams that are required.
                Params include: ["clouds", "radiation", "temp", "winddeg", "windspeed", "windspeed_100m", "pressure",
                "humidity", "rain", 'mxlD', 'snowDepth', 'clearSky_ulf', 'clearSky_dlf', 'ssrunoff']
            datetime_start (datetime) : start date time. Default: 14 days ago
            datetime_end (datetime): end date time. Default: now + 2 days.
            source (str or list of str): which weather models should be used.
                Options: "OWM", "DSN", "WUN", "harmonie", "harm_arome", "harm_arome_fallback", "icon", "optimum",
                Default: 'optimum'. This combines harmonie, harm_arome, icon and DSN,
                taking the (heuristicly) best available source for each moment in time
            resolution (str): Time resolution of the returned data, default: "15min"
            country (str): Country code (2-letter: ISO 3166-1). e.g. NL
            number_locations (int): number of weather locations desired
            type (str) : type of weather forecast (smallest_tAhead of multiple_tAheads)
        Returns:
            pd.DataFrame: The most recent weather prediction

        Example:
            client = influxdb.DataFrameClient()
            df = QueryWeatherData(client, location='Volkel',
                                  weatherParams=["clouds", "radiation"], source='optimum')
            print(df.head())
        """

        # Converting to datetime objet with tz attribute
        datetime_start = pd.to_datetime(datetime_start)
        datetime_end = pd.to_datetime(datetime_end)

        # Convert to UTC and remove UTC as note
        if datetime_start.tz is not None:
            datetime_start = datetime_start.tz_convert("UTC").tz_localize(None)

        if datetime_end.tz is not None:
            datetime_end = datetime_end.tz_convert("UTC").tz_localize(None)

        location_name = self._get_nearest_weather_locations(
            location=location, country=country, number_locations=number_locations
        )

        # Make a list of the source and weatherparams.
        # Like this, it also works if source is a string instead of multiple values
        if isinstance(source, str):
            source = [source]
        if isinstance(weatherparams, str):
            weatherparams = [weatherparams]

        # Try to get the data from influx.
        if "optimum" in source:
            # so the query return all
            source = Settings.optimum_weather_sources
            combine_sources = True
        else:
            combine_sources = False

        # Initialize binding params
        bind_params = {
            "_start": datetime_start,
            "_stop": datetime_end,
        }

        # Initialise strings for the querying influx, it is not possible to parameterize this string
        weather_params_str = '" or r._field == "'.join(weatherparams)
        weather_models_str = '" or r.source == "'.join(source)
        weather_location_name_str = '" or r.input_city == "'.join(
            location_name.to_list()
        )

        if type == "smallest_tAhead":
            weather_measurement_str = "weather"
            influx_indices = ["source", "input_city"]
            grouping_indices = ["source", "input_city"]
        elif type == "multiple_tAheads":
            weather_measurement_str = "weather_tAhead"
            influx_indices = ["source", "input_city", "tAhead"]
            grouping_indices = ["source", "input_city", "created"]

        # Create the query
        query = f"""
            from(bucket: "forecast_latest/autogen") 
                |> range(start: {bind_params["_start"].strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {bind_params["_stop"].strftime('%Y-%m-%dT%H:%M:%SZ')}) 
                |> filter(fn: (r) => r._measurement == "{weather_measurement_str}" and (r._field == "{weather_params_str}") and (r.source == "{weather_models_str}") and (r.input_city == "{weather_location_name_str}"))
        """

        # Execute Query
        result = _DataInterface.get_instance().exec_influx_query(query)

        # For multiple Fields a list is returned.
        if isinstance(result, list):
            result = pd.concat(result)[["_value", "_field", "_time", "source"]]

        # Check if response is empty
        if not result.empty:
            result = parse_influx_result(result, influx_indices)
        else:
            self.logger.warning("No weatherdata found. Returning empty dataframe")
            return pd.DataFrame(
                index=genereate_datetime_index(
                    start=datetime_start,
                    end=datetime_end,
                    freq=resolution,
                )
            )

        # Create a single dataframe with combined sources
        if combine_sources:
            self.logger.info("Combining sources into single dataframe")
            result = self._combine_weather_sources(result)
            result["source"] = "optimum"

        # Compute source_run
        if type == "multiple_tAheads":
            result["created"] = self._get_source_run(result.index, result.tAhead)

        # Interpolate if nescesarry by input_city, source (and tAhead)
        with pd.option_context("future.no_silent_downcasting", True):
            result = (
                result.groupby(grouping_indices)
                .resample(resolution, include_groups=False)
                .interpolate(limit=11)
                .reset_index(grouping_indices)
            )

        if number_locations == 1:
            result = result.drop(columns="input_city")
        else:
            # Adding distance information en km
            result = (
                result.reset_index()
                .merge(location_name.reset_index(), how="left", on="input_city")
                .set_index("datetime")
            )

        return result

    def get_datetime_last_stored_knmi_weatherdata(
        self, source: str = "harm_arome"
    ) -> datetime:
        """Returns datetime of latest KNMI run in influx Database. If no run is found return first unix time."""
        query = f"""
            from(bucket: "forecast_latest/autogen" )   
                |> range(start: - 2d)
                |> filter(fn: (r) => r._measurement == "weather" and r.source == "{source}" and r._field == "source_run")
                |> max()
        """
        result = _DataInterface.get_instance().exec_influx_query(query)

        if isinstance(result, pd.DataFrame) and not result.empty:
            # Get latest run
            latest_unix = result["_value"].max()
        else:
            latest_unix = 0  # Return first unix time if no run is found

        # unix timestamp of most recent stored weather forecast created
        last_stored_run = datetime.fromtimestamp(latest_unix, pytz.utc)
        return last_stored_run
