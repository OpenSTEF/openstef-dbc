# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from typing import Tuple, List
from datetime import datetime
import time
import re

import numpy as np
import pandas as pd

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.log import logging


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
        forecast["created"] = int(datetime.utcnow().timestamp())

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
            "created": np.int64,
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
        desired_t_aheads = [0.0, 1.0, 4.0, 8.0, 24.0, 47.0, 50.0, 144.0]

        t_adf = forecast.copy()

        t_adf["tAhead"] = np.floor(
            (t_adf.index.tz_localize(None) - datetime.utcnow()).total_seconds() / 3600
        )
        # floor all tAheads to first tAhead lower than or equal to calculated tAhead
        # 1000 as a fill value if all else fails
        # !TODO check this!
        t_adf["tAhead"] = [
            min([1000.0] + [float(x) for x in desired_t_aheads if x >= t_ahead])
            for t_ahead in t_adf["tAhead"]
        ]
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

    def write_weather_data(
        self,
        data: pd.DataFrame,
        source: str,
        table: str = "weather",
        dbname: str = "forecast_latest",
        tag_columns: List[str] = None,
    ):
        """This function should be used to write data directly to our database.
        Do not use this function for Measurements data (pvdata, demanddata, winddata).
        Using the PostData function is prefered for this data, since traffic monitoring is ensured.

        Parameters:
            - data: pd.DataFrame(index = "datetimeFC", columns = ['input_city','temp','windspeed'])
            - source: str: source of the weatherdata. Obligated!
            - table: str
            - insertIgnore: Bool
            - tag_columns: list: the column names used as tags in influx

        Output:
            message: string
        """

        # These names are forbidden to be used as fields, as thy will break everything!
        # Influx is very picky about fields vs tags
        if tag_columns is None:
            tag_columns = ["input_city", "source"]

        forbidden_fields = ["input_city", "source"]

        message = ""  # Create string placeholder for return message

        self.logger.info("Write weather data to database")

        influx_df = data.copy()
        influx_df["source"] = source
        # Add created to data
        influx_df["created"] = int(datetime.utcnow().timestamp())

        field_columns = [
            x
            for x in list(influx_df.columns)
            if x not in tag_columns + forbidden_fields
        ]

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
            message += "Written " + str(len(influx_df)) + " rows to influx"
            self.logger.info(
                "Succesfully written fields to database", field_columns=field_columns
            )
            self.logger.info(
                "Successfully written tags to database", tag_columns=tag_columns
            )

        return message

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

        # Compose query for writing new systems
        query = "INSERT IGNORE INTO `systems` (sid, region) VALUES " + values

        # Execute query
        response = _DataInterface.get_instance().mysql_engine.execute(query)

        self.logger.info(
            "Added {} new systems to the systems table in the MySQL database".format(
                response.rowcount
            )
        )

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
