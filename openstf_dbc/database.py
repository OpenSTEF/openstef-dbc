from ktpbase.data_interface import _DataInterface
from ktpbase.services.prediction_job import PredictionJob
from ktpbase.services.weather import Weather
from ktpbase.services.ems import Ems
from ktpbase.services.predictor import Predictor
from ktpbase.services.write import Write
from ktpbase.services.splitting import Splitting
from ktpbase.services.predictions import Predictions
from ktpbase.services.model_input import ModelInput
from ktpbase.services.systems import Systems


class DataBase:
    """Provides a high-level interface to various data sources.

        All user/client code should use this class to get or write data. Under the hood
        this class uses various services to interfact with its datasource.
    """

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
    get_datetime_last_stored_knmi_weatherdata = _weather.get_datetime_last_stored_knmi_weatherdata
    # predictor methods
    get_apx = _predictor.get_apx
    get_gas_price = _predictor.get_gas_price
    get_tdcv_load_profiles = _predictor.get_tdcv_load_profiles
    # historic cdb data service
    get_load_sid = _historic_cdb_data_service.get_load_sid
    get_load_created_after = _historic_cdb_data_service.get_load_created_after
    get_load_pid = _historic_cdb_data_service.get_load_pid
    get_states_flexnet = _historic_cdb_data_service.get_states_flexnet
    get_curtailments = _historic_cdb_data_service.get_curtailments
    get_load_created_datetime_sid = _historic_cdb_data_service.get_load_created_datetime_sid
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
    get_pv_systems_with_incorrect_location = _systems.get_pv_systems_with_incorrect_location
    get_random_pv_systems = _systems.get_random_pv_systems

    def __init__(self):
        """Init the stuff that also performs actions on init"""
        # Ktp api
        self.ktp_api = _DataInterface.get_instance().ktp_api

