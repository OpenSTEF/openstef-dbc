# -*- coding: utf-8 -*-
import pandas as pd
from unittest import mock
from pathlib import Path

from pandas.testing import assert_frame_equal
from openstf_dbc.services.weather import Weather


def __init__(self, *args):
    self.sql_engine = lambda x: None
    self.sql_engine.execute = mock.MagicMock()


DATA_FOLDER = Path(__file__).absolute().parent.parent / "data"
read_csv_kwargs = {"sep": ";", "index_col": 0, "parse_dates": ["datetime"]}

noncombined_weatherdata = pd.read_csv(
    DATA_FOLDER / "noncombined_weatherdata_test_data.csv", **read_csv_kwargs
)

combined_weatherdata = pd.read_csv(
    DATA_FOLDER / "combined_weatherdata_test_data.csv", **read_csv_kwargs
)

noncombined_weatherdata_nomissing = pd.read_csv(
    DATA_FOLDER / "noncombined_weatherdata_nomissing_test_data.csv", **read_csv_kwargs
)

combined_weatherdata_nomissing = pd.read_csv(
    DATA_FOLDER / "combined_weatherdata_nomissing_test_data.csv", **read_csv_kwargs
)

combined_weatherdata_DSN = pd.read_csv(
    DATA_FOLDER / "combined_weatherdata_DSN_test_data.csv", **read_csv_kwargs
)

# change dtypes to object
combined_weatherdata_DSN.pressure = combined_weatherdata_DSN.pressure.astype(str)
combined_weatherdata_DSN.winddeg = combined_weatherdata_DSN.winddeg.astype(str)


@mock.patch.object(Weather, "__init__", __init__)
def test_fill_nan_values():
    """Data: dataframe contains weather data of multiple sources for same timpestamp with nan-values

    Expected: dataframe without duplicate timestamps containing data from multiple data sources without nan-values
    """
    database = Weather()
    response = database._combine_weather_sources(result=noncombined_weatherdata)
    expected_response = combined_weatherdata
    assert_frame_equal(expected_response, response)


@mock.patch.object(Weather, "__init__", __init__)
def test_no_nan_values():
    """Data: dataframe contains weather data of multiple sources for same timpestamp without nan-values

    Expected: dataframe containing data only from preferred data source. No duplicate timestamps
    """
    database = Weather()
    response = database._combine_weather_sources(
        result=noncombined_weatherdata_nomissing
    )
    expected_response = combined_weatherdata_nomissing
    assert_frame_equal(expected_response, response)


@mock.patch.object(Weather, "__init__", __init__)
def test_different_source_order():
    """Data: dataframe contains weather data of multiple sources for same timpestamp with nan-values

    Expected: dataframe without duplicate timestamps containing data from mostly DSN data source without nan-values
    """
    database = Weather()
    response = database._combine_weather_sources(
        result=noncombined_weatherdata, source_order=["DSN", "harmonie", "harm_arome"]
    )
    expected_response = combined_weatherdata_DSN
    assert_frame_equal(expected_response, response)


@mock.patch.object(Weather, "__init__", __init__)
def test_non_optimum_source():
    """Data: dataframe contains weather data of one source

    Expected: return same dataframe
    """
    database = Weather()
    response = database._combine_weather_sources(result=combined_weatherdata)
    expected_response = combined_weatherdata
    assert_frame_equal(expected_response, response)


# Run all tests
if __name__ == "__main__":
    tests = [eval(item) for item in dir() if item[0 : len("test_")] == "test_"]
    for test in tests:
        print("executing:", test)
        test()
    print("Succes!")
