# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from typing import Tuple, List, Dict
from datetime import datetime
import time
import re

import numpy as np
import pandas as pd

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.log import logging
from openstef_dbc.utils import round_down_time_differences


class Write:
    def __init__(self):
        self.logger = logging.get_logger(self.__class__.__name__)

    def write_location(self, location_name: str, location: Tuple[float, float]) -> None:
        bind_params = {
            "table_name": "NameToLatLon",
            "loc": location_name,
            "lat": location[0],
            "lon": location[1],
        }

        statement = "INSERT INTO %(table_name)s (regionInput, lat,lon) VALUES (%(loc)s, %(lat)s, %(lon)s)"

        _DataInterface.get_instance().exec_sql_write(statement, params=bind_params)

        self.logger.info("Added {location_name} to {table_name} table")

    def write_forecast(
        self,
        data: pd.DataFrame,
        dbname: str = "forecast_latest",
        influxtable: str = "prediction",
        t_ahead_series: bool = False,
        force_quality: bool = True,
    ) -> str:
        """Write a Forecast to the database.

        This function should be used to write data directly to our database.
        Do not use this function for Measurements data (pvdata, demanddata, winddata).
        Using the PostData function is preffered for this data, since traffic monitoring is ensured.

        The field 'quality' indicates if a forecasted value is:
            'actual','substituted', or 'not_renewed'

        Args:
            data (pd.DataFrame): pd.DataFrame(index="datetimeFC", columns=[
                'forecast','pid',('stdev','description','customer','quality')]
            dbname (str): The database name. Default "forecast_latest".
            influxtable (str): The table name. Default "prediction".
            t_ahead_series (bool): Should tAhead be stored to influx for specific tAheads
            force_quality (bool): If True, add a column quality with the field actual,
                or replace nan's of the column quality to actual

        Returns:
            (str): Empty if succes
        """

        # Create string placeholder for return message

        self.logger.info("Write forecast to database")

        forecast = data.copy()
        # Add created to data
        forecast["created"] = str(int(datetime.utcnow().timestamp()))

        # Add quality column. Fill nan's or missing column with 'actual'
        if force_quality:
            if "quality" not in forecast.columns:
                forecast["quality"] = "actual"
            else:
                forecast.loc[forecast["quality"].isna(), "quality"] = "actual"

        # Write DataFrame to influx database
        # Find tag columns
        tag_columns = ["pid", "type", "customer"]
        # Specify field columns
        field_columns = [x for x in forecast.columns if x not in tag_columns]

        # Cast columns to correct type, as influx is extremely picky
        casting_dict = {
            "prediction": np.float64,
            "stdev": np.float64,
            "pid": np.int64,
            "type": str,
            "customer": str,
            "created": str,
            "algtype": str,
            "description": str,
            "forecast_solar": np.float64,
            "forecast_wind_on_shore": np.float64,
            "forecast_other": np.float64,
            "forecast": np.float64,
            "quality": str,
            "tAhead": np.float64,
        }
        # Generate casting dict for available quantiles
        p = re.compile(r"quantile_P\d\d")
        quantile_columns = [s for s in field_columns if p.match(s)]
        casting_dict.update(dict.fromkeys(quantile_columns, np.float64))

        # Check if we have all columns and not some extra
        casting_dict_columns = list(casting_dict.keys())
        for k in casting_dict_columns:
            # Remove any casting dict entries that are not in the dataframe
            if k not in forecast.columns:
                casting_dict.pop(k, None)

        if set(casting_dict.keys()) != set(
            forecast.columns.to_list()
        ):  # Check if we have a type description for every column, if not raise an error
            raise ValueError(
                "Got unexpected columns, unable to cast these columns in the correct datatype."
            )

        forecast = forecast.astype(casting_dict)

        result = _DataInterface.get_instance().exec_influx_write(
            forecast.copy(),
            database=dbname,
            measurement=influxtable,
            tag_columns=tag_columns,
            field_columns=field_columns,
            time_precision="s",
        )

        message = ""

        if result:
            num_rows = str(len(forecast))
            self.logger.info(
                "Succesfully written Forecast to database",
                num_rows=num_rows,
                database=dbname,
                measurement=influxtable,
                tag_columns=tag_columns,
                field_columns=field_columns,
            )
            message += f"Written {num_rows} rows to {dbname}.{influxtable}"

        # If desired, write tAhead series to influx - tAhead table
        if t_ahead_series:
            if len(forecast) == 0:
                message += "Len forecasts=0, not going to write them to tAheads"
                return message
            message += self._write_t_ahead_series(forecast=forecast, dbname=dbname)

        return message

    def _write_t_ahead_series(
        self, forecast: pd.DataFrame, dbname: str = "forecast_latest"
    ) -> str:
        allowed_columns = [
            "tAhead",
            "pid",
            "forecast",
            "stdev",
            "customer",
            "description",
            "type",
            "quality",
        ]

        # Extract quantile column names and add to allowed columns
        quantile_forecasts = [name for name in forecast.columns if "quantile_" in name]
        allowed_columns = allowed_columns + quantile_forecasts

        influxtable = "prediction_tAheads"

        # specify desired t_aheads
        desired_t_aheads = [0.0, 1.0, 4.0, 8.0, 15.0, 24.0, 39.0, 47.0, 50.0, 144.0]

        t_adf = forecast.copy()

        # Calculate tAheads
        timediffs = (
            t_adf.index.tz_localize(None) - datetime.utcnow()
        ).total_seconds() / 3600
        # Round it to the first bigger desired_t_ahead
        t_adf["tAhead"] = round_down_time_differences(timediffs, desired_t_aheads)

        t_adf = t_adf.loc[
            [x in desired_t_aheads for x in t_adf.tAhead],
            [x for x in allowed_columns if x in t_adf.columns],
        ]
        tag_columns = ["pid", "customer", "type", "tAhead"]
        field_columns = [x for x in t_adf.columns if x not in tag_columns]

        # Force a hard typecast so floats are definetly stored as floats!
        float_columns = ["tAhead", "forecast", "stdev"] + quantile_forecasts
        float_columns_in_dataframe = [
            df_column for df_column in t_adf.columns if df_column in float_columns
        ]
        t_adf[float_columns_in_dataframe] = t_adf[float_columns_in_dataframe].apply(
            pd.to_numeric, downcast="float", errors="coerce"
        )

        result = _DataInterface.get_instance().exec_influx_write(
            t_adf.copy(),
            database=dbname,
            measurement=influxtable,
            tag_columns=tag_columns,
            field_columns=field_columns,
            time_precision="s",
        )
        if not result:
            return ""

        num_rows = str(len(t_adf))
        self.logger.info(
            "Succesfully written t ahead series to database",
            num_rows=num_rows,
            database=dbname,
            measurement=influxtable,
            tag_columns=tag_columns,
            field_columns=field_columns,
        )
        return f"Written {num_rows} rows to {dbname}.{influxtable}"

    def _write_weather_forecast_data_to_influx(
        self,
        influx_df: pd.DataFrame,
        dbname: str,
        table: str,
        tag_columns: List[str],
        field_columns: List[str],
        casting_dict: Dict[str, type],
    ):
        """Writes weather forecasts to influx database after casting the columns to the correct datatype.

        Args:
            influx_df: pd.DataFrame(index = "datetimeFC", columns = ['input_city','temp','windspeed'])
            dbname: (str) database name
            table: (str) table name
            tag_columns: (list) the column names used as tags in influx
            field_columns: (list) the column names used as fields in influx
            casting_dict: (dict) dictionary with column names as keys and the desired datatype as values

        Returns:
            None
        """
        # Check if we have all columns and not some extra
        casting_dict_columns = list(casting_dict.keys())
        for k in casting_dict_columns:
            # Remove any casting dict entries that are not in the dataframe
            if k not in influx_df.columns:
                casting_dict.pop(k, None)

        if set(casting_dict.keys()) != set(
            influx_df.columns.to_list()
        ):  # Check if we have a type description for every column, if not raise an error
            raise ValueError(
                "Got unexpected columns, unable to cast these columns in the correct datatype."
            )

        influx_df = influx_df.astype(casting_dict)

        # Write DataFrame to influx database
        result = _DataInterface.get_instance().exec_influx_write(
            influx_df.copy(),
            database=dbname,
            measurement=table,
            tag_columns=tag_columns,
            field_columns=field_columns,
            time_precision="s",
        )

        if result:
            self.logger.info(
                "Succesfully written fields to database", field_columns=field_columns
            )
            self.logger.info(
                "Successfully written tags to database", tag_columns=tag_columns
            )
            self.logger.info("Written " + str(len(influx_df)) + " rows to influx")
        else:
            self.logger.error(
                "Could not write weather data to database",
                field_columns=field_columns,
                tag_columns=tag_columns,
            )

    def _write_weather_forecast_data_latest(
        self,
        data: pd.DataFrame,
        source: str,
        table: str = "weather",
        dbname: str = "forecast_latest",
        tag_columns: List[str] = None,
        casting_dict: Dict[str, type] = {},
    ):
        """Writes weather data to the database. This function writes the data to a table containing the latest forecasts.

        Args:
            data: pd.DataFrame(index = "datetimeFC", columns = ['input_city','temp','windspeed'])
            source: (str) source of the weatherdata
            table: (str) table name
            dbname: (str) database name
            tag_columns: (list) the column names used as tags in influx
            casting_dict: (dict) dictionary with column names as keys and the desired datatype as values

        Returns:
            None
        """

        influx_df = data.copy()
        influx_df["source"] = source
        # Add created to data
        influx_df["created"] = int(datetime.utcnow().timestamp())

        if tag_columns is None:
            tag_columns = ["input_city", "source"]

        field_columns = [x for x in list(influx_df.columns) if x not in tag_columns]

        self._write_weather_forecast_data_to_influx(
            influx_df,
            dbname,
            table,
            tag_columns,
            field_columns,
            casting_dict,
        )

    def _write_weather_forecast_data_t_ahead(
        self,
        data: pd.DataFrame,
        source: str,
        table: str = "weather",
        dbname: str = "forecast_latest",
        tag_columns: List[str] = None,
        casting_dict: Dict[str, type] = {},
        desired_t_aheads: List[float] = [
            1.0,
            12.0,
            15.0,
            24.0,
            36.0,
            39.0,
            48.0,
            4 * 24.0,
            6 * 24.0,
        ],
    ):
        """Write weather data to the database. This function writes the data to a table containing with the forecasts for different t_ahead values.

        Args:
            data: pd.DataFrame(index = "datetimeFC", columns = ['input_city','temp','windspeed'])
            source: (str) source of the weatherdata
            table: (str) table name
            dbname: (str) database name
            tag_columns: (list) the column names used as tags in influx
            casting_dict: (dict) dictionary with column names as keys and the desired datatype as values
            desired_t_aheads: (list) the t_ahead values for which the data should be written

        Returns:
            None
        """

        influx_df = data.copy()
        influx_df["source"] = source
        # Add created to data
        influx_df["created"] = int(datetime.utcnow().timestamp())

        if tag_columns is None:
            tag_columns = ["input_city", "source"]
        tag_columns.append("tAhead")

        # Calculate tAheads
        timediffs = (
            influx_df.index.tz_localize(None) - datetime.utcnow()
        ).total_seconds() / 3600
        # Round it to the first bigger desired_t_ahead
        influx_df["tAhead"] = round_down_time_differences(timediffs, desired_t_aheads)

        # Remove the undesired t_aheads
        influx_df = influx_df.loc[
            [x in desired_t_aheads for x in influx_df.tAhead],
            [x for x in influx_df.columns],
        ]
        # Ignore tag columns when determining field columns
        field_columns = [x for x in influx_df.columns if x not in tag_columns]

        # Set correct casting type for tAhead column
        casting_dict["tAhead"] = np.float64

        self._write_weather_forecast_data_to_influx(
            influx_df,
            dbname,
            table,
            tag_columns,
            field_columns,
            casting_dict,
        )

    def write_weather_forecast_data(
        self,
        data: pd.DataFrame,
        source: str,
        table: str = "weather",
        dbname: str = "forecast_latest",
        tag_columns: List[str] = None,
    ):
        """Write weather forecast data to the database.
        This function writes the data both to a table containing the latest forecasts,
        and a similar table with suffix _tAheads with the forecasts for different t_ahead values.

        Args:
            data: pd.DataFrame(index = "datetimeFC", columns = ['input_city','temp','windspeed'])
            source: (str) source of the weatherdata
            table: (str) table name
            dbname: (str) database name
            tag_columns: (list) the column names used as tags in influx

        Returns:
            None
        """

        # Cast columns to correct type, as influx is extremely picky
        casting_dict = {
            "source_run": np.int64,
            "input_city": str,
            "temp": np.float64,
            "windspeed_100m": np.float64,
            "windspeed": np.float64,
            "winddeg": np.float64,
            "clouds": np.float64,
            "mxlD": np.float64,
            "snowDepth": np.float64,
            "pressure": np.float64,
            "humidity": np.float64,
            "clearSky_ulf": np.float64,
            "clearSky_dlf": np.float64,
            "radiation": np.float64,
            "radiation_direct": np.float64,
            "radiation_diffuse": np.float64,
            "radiation_normal": np.float64,
            "windspeed_100m_ensemble": np.float64,
            "windspeed_ensemble": np.float64,
            "winddeg_ensemble": np.float64,
            "clouds_ensemble": np.float64,
            "radiation_ensemble": np.float64,
            "ensemble_run": str,
            "source": str,
            "created": np.int64,
        }

        self._write_weather_forecast_data_latest(
            data=data,
            source=source,
            table=table,
            dbname=dbname,
            tag_columns=tag_columns,
            casting_dict=casting_dict,
        )
        self._write_weather_forecast_data_t_ahead(
            data=data,
            source=source,
            table=table + "_tAheads",
            dbname=dbname,
            casting_dict=casting_dict,
            tag_columns=tag_columns,
        )

    def write_realised(self, df: pd.DataFrame, sid: str):
        """Method that writes measurement data to the influx database.

        Args:
            df: pd.DataFrame(index = "datetime", columns = ['output'])
            sid: (str) String with system id of the measurement

        Returns:
            None
        """
        df["type"] = "measurement"
        df["system"] = str(sid)
        df["created"] = int(time.time())
        df = df.astype({"output": np.float64})
        # Write to influx database
        result = _DataInterface.get_instance().exec_influx_write(
            df,
            database="realised",
            measurement="power",
            tag_columns=["system", "type"],
            field_columns=["output", "created"],
            time_precision="s",
        )
        if not result:
            self.logger.error(
                "Something wend wrong while writing measurement data to influx"
            )
            return

        self.logger.info(
            "Wrote measurement data for {} systems to influx".format(
                df["system"].nunique()
            )
        )

    def write_realised_pvdata(self, df: pd.DataFrame, region: str) -> None:
        """Method that writes realised pv data to the influx database. This function
        also adds systems to the systems table in mysql if they do not yet excist.

        Args:
            df: pd.DataFrame(index = "datetime", columns = ['output','system'])
            region: (str) String with the pvdata.org region to which the systems in the df belong

        Returns:
            None
        """
        df["type"] = "solar"
        df["created"] = int(time.time())
        df = df.astype({"output": np.float64})
        # Write to influx database
        result = _DataInterface.get_instance().exec_influx_write(
            df,
            database="realised",
            measurement="power",
            tag_columns=["system", "type"],
            field_columns=["output", "created"],
            time_precision="s",
        )
        if not result:
            self.logger.error("Something wend wrong while writing pvdata to influx")
            return

        self.logger.info(
            "Wrote pv data for {} systems to influx".format(df["system"].nunique())
        )

        # Prepare dataframe for writing systems to the systems table in mysql
        systems_for_sql = df[["system"]].drop_duplicates()

        # Define placeholder string
        values = ""

        # Fill string with values
        for sid in systems_for_sql["system"]:
            values += "('" + sid + "', '" + region + "')" + ","

        # Get rid of last comma
        values = values[0:-1]

        db_type = _DataInterface.get_instance().get_sql_db_type()

        # Compose query for writing new systems
        if db_type == "mysql":
            # Compose query for writing new systems in MySQL
            query = "INSERT IGNORE INTO `systems` (sid, region) VALUES " + values
        elif db_type == "postgresql":
            # Compose query for writing new systems in PostgreSQL
            query = (
                "INSERT INTO systems (sid, region) VALUES "
                + values
                + " ON CONFLICT (sid) DO NOTHING"
            )
        else:
            self.logger.error("Unsupported database type: {}".format(db_type))
            return

        # Execute query
        _DataInterface.get_instance().exec_sql_write(query)

    def write_kpi(self, pj: dict, kpis: dict) -> None:
        """Method that writes the key performance indicators of a pid to an influx DB.
        Note that NaN / Inf are converted to 0, since these are not supported in Influx.

        Args:
            pj: a KTP prediction job
            kpis: Dictionary with dictionaries with Kpis for each t_ahead

        Returns:
            None

        """
        df = pd.DataFrame(pd.DataFrame(kpis)).T
        df["tAhead"] = [float(x.replace("h", "")) for x in df.index]
        df["pid"] = pj["id"]
        # add date
        df = df.set_index("date")

        # Convert nan / inf since these are not supported in influx
        df = df.replace([np.inf], 9999.9)
        df = df.replace([-np.inf], -9999.9)
        df = df.fillna(value=0)

        # Specify correct types
        intcols = ["pid"]
        floatcols = [x for x in df.columns if x not in intcols]
        df[floatcols] = df[floatcols].astype("float64")
        df[intcols] = df[intcols].astype("Int32")

        # Add user-friendly-fields
        df["description"] = pj["description"]
        df["customer"] = pj["name"]

        # Initialise the tag columns for Influx
        allowed_tags = ["pid", "tAhead", "description", "customer", "window_days"]
        tags = [x for x in allowed_tags if x in df.columns]

        # Write the kpis to the influx db
        _DataInterface.get_instance().exec_influx_write(
            df,
            database="forecast_latest",
            measurement="prediction_kpi",
            tag_columns=tags,
        )
        # Let user know everything went well
        self.logger.info("Succesfully wrote KPIs for pid: {}".format(str(pj["id"])))

    def write_apx_market_data(self, apx_data: pd.DataFrame) -> bool:
        success = _DataInterface.get_instance().exec_influx_write(
            apx_data,
            database="forecast_latest",
            measurement="marketprices",
            tag_columns=["Name"],
            field_columns=["Price"],
            time_precision="s",
        )
        return success

    def write_sjv_load_profiles(
        self, sjv_load_data: pd.DataFrame, field_columns: List[str]
    ) -> bool:
        success = _DataInterface.get_instance().exec_influx_write(
            sjv_load_data,
            database="realised",
            measurement="sjv",
            tag_columns=["year_created"],
            field_columns=field_columns,
            time_precision="s",
        )
        return success

    def write_windturbine_powercurves(
        self, power_curves: pd.DataFrame, if_exists: str = "fail"
    ) -> None:
        _DataInterface.get_instance().exec_sql_dataframe_write(
            power_curves, "genericpowercurves", if_exists=if_exists, index=False
        )

    def write_energy_splitting_coefficients(
        self, coefficients: dict, if_exists: str = "fail"
    ) -> None:
        _DataInterface.get_instance().exec_sql_dataframe_write(
            coefficients, "energy_split_coefs", if_exists=if_exists, index=False
        )
