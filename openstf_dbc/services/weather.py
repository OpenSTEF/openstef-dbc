# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime, timedelta

import geopy
import geopy.distance
import numpy as np
import pandas as pd
import pytz
import structlog
from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.services.write import Write
from openstf_dbc.utils import genereate_datetime_index


class Weather:
    def __init__(self) -> None:
        self.logger = structlog.get_logger(self.__class__.__name__)

    def get_weather_forecast_locations(self, country="NL", active=1):
        """Get weather forecast locations.

        Returns:
            List[dict]: List of locations with keys:
                ["city"]:       City of the weather forecast.
                ["lat"]:        Lattitude coordinate.
                ["lon"]:        Longitude coordinate.
                ["country"]:    Country code (2-letter: ISO 3166-1)
        """
        query = f"""
            SELECT input_city as city, lat, lon, country
            FROM weatherforecastlocations
            WHERE country = '{country}' AND active = {active}
        """
        result = _DataInterface.get_instance().exec_sql_query(query)

        locations = result.to_dict(orient="records")

        return locations

    def _get_nearest_weather_location(
        self, location, threshold=150, country="NL", active=1
    ):
        """Find the nearest weather forecast location.

        Function that, given an location, finds the nearest location for which a
        weatherforecast is available. A warning is generated when the distance is
        greater than a certain distance (threshold).

        If multiple weather locations have the same distance, only one is returned.

        Args:
            location (str, tuple): Name of the location/city or coordinates (lat, lon).
            threshold (int): Maximum distance [km] before a warning is generated.
            country (str):  Country code (2-letter: ISO 3166-1).
            active (int): Use only active weather location if 1.

        Returns:
            str: The name of the weather forecast location.
        """
        # Get all available cities
        weather_locations = self.get_weather_forecast_locations(country="NL", active=1)

        # If location is string, convert to (lat, lon)
        if type(location) is str:
            location_coordinates = self._get_coordinates_of_location(location)
        else:
            location_coordinates = location

        # Now we have the coordinates of the input_city. Next, find nearest weather location
        distances = pd.DataFrame(columns=["distance", "input_city"])
        for weather_location in weather_locations:
            coordinates = (weather_location["lat"], weather_location["lon"])

            if np.nan in coordinates:
                raise ValueError(
                    "No coordinates found, pleas make sure a prope location "
                    "is configured in the mySQL database"
                )

            distance = round(
                geopy.distance.geodesic(coordinates, location_coordinates).km
            )
            city = weather_location["city"]
            distances = distances.append(
                {"distance": distance, "input_city": city}, ignore_index=True
            )

        distances = distances.set_index("distance")

        nearest_location = distances["input_city"][distances.index.min()]

        # Find closest weather location
        if distances.index.min() < threshold:

            if isinstance(nearest_location, pd.Series):
                return nearest_location.reset_index(drop=True)[0]
            else:
                return nearest_location

        raise Warning(
            "Closest weather location is farther than threshold: {dist} > {threshold}".format(
                dist=str(min(distances)), threshold=str(threshold)
            )
        )

    def _get_coordinates_of_location(self, location_name):
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
        query = 'SELECT lat, lon from NameToLatLon where regionInput = "{city}"'.format(
            city=location_name
        )
        location = _DataInterface.get_instance().exec_sql_query(query)

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

    def _combine_weather_sources(self, result, source_order=None):
        """
        Function that complete's the weatherdata from the influx database retrieved using get_weatherdata
        Additionally, weatherdata from several sources is combined to a single forecast.
        The preferred source_order is given as argument.

        Args:
            result (pd.DataFrame): return value from the function 'get_weatherdata'
            source_order (list): which weather models should be used (first).
                Options: "OWM", "DSN", "WUN", "harmonie", "harm_arome", "optimum"
                Default: '"harm_arome", "harmonie", "DSN"'. This combines harmonie, harm_arome and DSN,
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
            source_order = ["harm_arome", "GFS_50", "harmonie", "DSN"]

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
        location,
        weatherparams,
        datetime_start=None,
        datetime_end=None,
        source="optimum",
        resolution="15min",
    ):
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
                Options: "OWM", "DSN", "WUN", "harmonie", "harm_arome", "optimum"
                Default: 'optimum'. This combines harmonie, harm_arome and DSN,
                taking the (heuristicly) best available source for each moment in time
            resolution (str): Time resolution of the returned data, default: "15T"
        Returns:
            pd.DataFrame: The most recent weather prediction

        Example:
            client = influxdb.DataFrameClient()
            df = QueryWeatherData(client, location='Volkel',
                                  weatherParams=["clouds", "radiation"], source='optimum')
            print(df.head())
        """

        if datetime_start is None:
            datetime_start = datetime.utcnow() - timedelta(days=14)

        datetime_start = pd.to_datetime(datetime_start)

        if datetime_end is None:
            datetime_end = datetime.utcnow() + timedelta(days=2)

        datetime_end = pd.to_datetime(datetime_end)

        # Convert to UTC and remove UTC as note
        if datetime_start.tz is not None:
            datetime_start = datetime_start.tz_convert("UTC").tz_localize(None)

        if datetime_end.tz is not None:
            datetime_end = datetime_end.tz_convert("UTC").tz_localize(None)

        # Get data from an hour earlier to correct for radiation shift later
        datetime_start_original = datetime_start.tz_localize("UTC")

        datetime_start -= timedelta(hours=1)

        location_name = self._get_nearest_weather_location(location)

        # Make a list of the source and weatherparams.
        # Like this, it also works if source is a string instead of multiple values
        if isinstance(source, str):
            source = [source]
        if isinstance(weatherparams, str):
            weatherparams = [weatherparams]

        # Try to get the data from influx.
        if "optimum" in source:
            # so the query return all
            source = ["harm_arome", "GFS_50", "harmonie", "DSN"]
            combine_sources = True
        else:
            combine_sources = False

        # Initialise strings for the querying influx
        weather_params_str = '", "'.join(weatherparams)
        weather_models_str = "' OR source::tag = '".join(source)

        # Create the query
        query = "SELECT source::tag, input_city::tag, \"{weather_params}\" FROM \
            \"forecast_latest\"..\"weather\" WHERE input_city::tag = '{location}' AND \
            time >= '{start}' AND time <= '{end}' AND (source::tag = '{weather_models}')".format(
            weather_params=weather_params_str,
            location=location_name,
            start=datetime_start,
            end=datetime_end,
            weather_models=weather_models_str,
        )
        # Execute Query
        result = _DataInterface.get_instance().exec_influx_query(query)

        if result:
            result = result["weather"]
            result.index.name = "datetime"
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

        # Interpolate if nescesarry
        result = result.resample(resolution).interpolate(limit=11)

        # Shift radiation by 30 minutes if resolution allows it
        if "radiation" in weatherparams:
            shift_delta = timedelta(minutes=30)
            if shift_delta % pd.Timedelta(resolution) == timedelta(0):
                result["radiation"] = result["radiation"].shift(1, shift_delta)

        # Drop extra rows not neccesary
        result = result.loc[datetime_start_original:]

        return result

    def get_datetime_last_stored_knmi_weatherdata(self):
        query = """
            SELECT * FROM forecast_latest..weather
            WHERE source::tag = 'harm_arome'
            ORDER BY time desc limit 1
        """
        result = _DataInterface.get_instance().exec_influx_query(query)
        latest = result["weather"]
        last_stored_run = datetime.fromtimestamp(latest["created"], pytz.utc)
        return last_stored_run
