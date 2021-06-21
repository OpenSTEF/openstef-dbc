# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from openstf_dbc.data_interface import _DataInterface


class Predictor:
    def get_apx(self, datetime_start, datetime_end):
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

    def get_tdcv_load_profiles(self, datetime_start, datetime_end):
        """Get TDCV load profiles.

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
