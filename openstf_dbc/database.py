# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from openstf_dbc import Singleton
from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.services.ems import Ems
from openstf_dbc.services.model_input import ModelInput
from openstf_dbc.services.prediction_job import PredictionJob
from openstf_dbc.services.predictions import Predictions
from openstf_dbc.services.predictor import Predictor
from openstf_dbc.services.splitting import Splitting
from openstf_dbc.services.systems import Systems
from openstf_dbc.services.weather import Weather
from openstf_dbc.services.write import Write


class DataBase(metaclass=Singleton):
    """Provides a high-level interface to various data sources.

    All user/client code should use this class to get or write data. Under the hood
    this class uses various services to interfact with its datasource.
    """

    _instance = None

    # services
    _write = Write()
    _prediction_job = PredictionJob()
    _weather = Weather()
    _historic_cdb_data_service = Ems()
    _predictor = Predictor()
    _splitting = Splitting()
    _predictions = Predictions()
    _model_input = ModelInput()
    _systems = Systems()

    # write methods
    write_weather_data = _write.write_weather_data
    write_realised_pvdata = _write.write_realised_pvdata
    write_hyper_params = _write.write_hyper_params
    write_kpi = _write.write_kpi
    write_forecast = _write.write_forecast
    write_apx_market_data = _write.write_apx_market_data
    write_sjv_load_profiles = _write.write_sjv_load_profiles
    write_windturbine_powercurves = _write.write_windturbine_powercurves
    write_energy_splitting_coefficients = _write.write_energy_splitting_coefficients
    # prediction job methods
    get_prediction_jobs_solar = _prediction_job.get_prediction_jobs_solar
    get_prediction_jobs_wind = _prediction_job.get_prediction_jobs_wind
    get_prediction_jobs = _prediction_job.get_prediction_jobs
    get_prediction_job = _prediction_job.get_prediction_job
    get_hyper_params = _prediction_job.get_hyper_params
    get_hyper_params_last_optimized = _prediction_job.get_hyper_params_last_optimized
    get_featureset = _prediction_job.get_featureset
    get_featuresets = _prediction_job.get_featuresets
    get_featureset_names = _prediction_job.get_featureset_names
    # weather methods
    get_weather_forecast_locations = _weather.get_weather_forecast_locations
    get_weather_data = _weather.get_weather_data
    get_datetime_last_stored_knmi_weatherdata = (
        _weather.get_datetime_last_stored_knmi_weatherdata
    )
    # predictor methods
    get_predictors = _predictor.get_predictors
    get_electricity_price = _predictor.get_electricity_price
    get_gas_price = _predictor.get_gas_price
    get_load_profiles = _predictor.get_load_profiles
    # historic cdb data service
    get_load_sid = _historic_cdb_data_service.get_load_sid
    get_load_created_after = _historic_cdb_data_service.get_load_created_after
    get_load_pid = _historic_cdb_data_service.get_load_pid
    get_states_flexnet = _historic_cdb_data_service.get_states_flexnet
    get_curtailments = _historic_cdb_data_service.get_curtailments
    get_load_created_datetime_sid = (
        _historic_cdb_data_service.get_load_created_datetime_sid
    )
    # splitting methods
    get_wind_ref = _splitting.get_wind_ref
    get_energy_split_coefs = _splitting.get_energy_split_coefs
    get_input_energy_splitting = _splitting.get_input_energy_splitting
    # predictions methods
    get_predicted_load = _predictions.get_predicted_load
    get_predicted_load_tahead = _predictions.get_predicted_load_tahead
    # model input methods
    get_model_input = _model_input.get_model_input
    get_wind_input = _model_input.get_wind_input
    get_power_curve = _model_input.get_power_curve
    get_solar_input = _model_input.get_solar_input
    # systems methods
    get_systems_near_location = _systems.get_systems_near_location
    get_systems_by_pid = _systems.get_systems_by_pid
    get_pv_systems_with_incorrect_location = (
        _systems.get_pv_systems_with_incorrect_location
    )
    get_random_pv_systems = _systems.get_random_pv_systems

    def __init__(self, config):
        """Construct the DataBase singleton.

        Initialize the datainterface and api. WARNING: this is a singleton class when
        calling multiple times with a config argument no new configuration will be
        applied.

        Args:
            config: Configuration object. with the following attributes:
                api.username (str): API username.
                api.password (str): API password.
                api.admin_username (str): API admin username.
                api.admin_password (str): API admin password.
                api.url (str): API url.
                influxdb.username (str): InfluxDB username.
                influxdb.password (str): InfluxDB password.
                influxdb.host (str): InfluxDB host.
                influxdb.port (int): InfluxDB port.
                mysql.username (str): MySQL username.
                mysql.password (str): MySQL password.
                mysql.host (str): MySQL host.
                mysql.port (int): MYSQL port.
                mysql.database_name (str): MySQL database name.
                proxies Union[dict[str, str], None]: Proxies.

        """

        self._datainterface = _DataInterface(config)
        # Ktp api
        self.ktp_api = self._datainterface.ktp_api

        DataBase._instance = self
