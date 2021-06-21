from pathlib import Path

import pandas as pd


class TestData:

    DATA_FILES_FOLDER = Path(__file__).parent.parent / "unit" / "data"

    LAG_FUNCTIONS_KEYS = [
        "T-1d",
        "T-2d",
        "T-3d",
        "T-4d",
        "T-5d",
        "T-6d",
        "T-7d",
        "T-8d",
        "T-9d",
        "T-10d",
        "T-11d",
        "T-12d",
        "T-13d",
        "T-14d",
    ]

    # (pickled) Dataframe attributes (will lazy load when required)
    TRAIN_INPUT_DATA = None
    INPUT_DATA = None
    INPUT_DATA_WITH_FEATURES = None
    INPUT_DATA_MULTI_HOR_FEATURES = None
    INPUT_DATA_HOLIDAY_FEATURES = None
    CORRECTED_FORECAST = None
    # Names of the (to be lazy loaded) pickled dataframes
    _PICKLED_DF_FILES = {
        "TRAIN_INPUT_DATA": "train_test_input_data.pickle",
        "INPUT_DATA": "input_xgboost_test.pickle",
        "INPUT_DATA_WITH_FEATURES": "output_xgboost_test_apply_features.pickle",
        "INPUT_DATA_WITH_FEATURES_2": "output_xgboost_test_holiday_features.pickle",
        "INPUT_DATA_MULTI_HOR_FEATURES": "output_xgboost_test_apply_multiple_horizon_features.pickle",
        "INPUT_DATA_HOLIDAY_FEATURES": "output_xgboost_test_holiday_features.pickle",
        "CORRECTED_FORECAST": "output_test_add_corrections.pickle",
    }
    # (csv) Dataframe attributes (will lazy load when required)
    FORCAST_DF = None
    STDEVBIAS_DF = None
    _CSV_DF_FILES = {
        "FORCAST_DF": "forecastdf_test_add_corrections.csv",
        "STDEVBIAS_DF": "stdevbiasdf_test_add_corrections.csv",
    }

    def __getattribute__(self, name):
        # lazy load pickled files
        if name in super().__getattribute__("_PICKLED_DF_FILES"):
            return self._get_pickled_dataframe(name)
        # Lazy load csv files
        if name in super().__getattribute__("_CSV_DF_FILES"):
            return self._get_csv_dataframe(name)

        return super().__getattribute__(name)

    def _get_pickled_dataframe(self, name):

        return self._load_stored_dataframe(name, self._PICKLED_DF_FILES, pd.read_pickle)


    def _get_csv_dataframe(self, name):
        return self._load_stored_dataframe(name, self._CSV_DF_FILES, pd.read_csv)

    def _load_stored_dataframe(self, name, filenames_map, read_func):
        df = super().__getattribute__(name)
        # if already loaded just return
        if df is not None:
            return df
        # if not loaded, load, store and return
        file_path = self.DATA_FILES_FOLDER / filenames_map[name]
        df = read_func(file_path)
        if name == "INPUT_DATA":
            df.index.freq="15T"
        setattr(self, name, df)
        return df
