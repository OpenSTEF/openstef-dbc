# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import json

from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.services.systems import Systems
from openstf_dbc.log import logging
from openstf_dbc.data.featuresets import FEATURESETS, FEATURESET_NAMES


class PredictionJob:
    def __init__(self):
        self.logger = logging.get_logger(self.__class__.__name__)

    def _add_description_to_prediction_job(self, prediction_job):
        return self._add_description_to_prediction_jobs([prediction_job])[0]

    def _add_description_to_prediction_jobs(self, prediction_jobs):

        for prediction_job in prediction_jobs:
            systems = Systems().get_systems_by_pid(
                pid=prediction_job["id"], return_list=True
            )
            systems_str = "+".join([s["system_id"] for s in systems])
            prediction_job["description"] = systems_str

        return prediction_jobs

    def _add_model_type_group_to_prediction_jobs(self, prediction_jobs):
        for prediction_job in prediction_jobs:
            # TODO this needs to be changed in the
            if "quantile" in prediction_job["model"]:
                prediction_job["model_type_group"] = "quantile"
            else:
                prediction_job["model_type_group"] = "default"

        return prediction_jobs

    def _add_model_type_group_to_prediction_job(self, prediction_job):
        return self._add_model_type_group_to_prediction_jobs([prediction_job])[0]

    def _add_quantiles_to_prediciton_job(self, prediction_job):
        return self._add_quantiles_to_prediciton_jobs([prediction_job])[0]

    def _add_quantiles_to_prediciton_jobs(self, prediction_jobs):
        prediction_job_ids = [pj["id"] for pj in prediction_jobs]
        prediction_jobs_ids_str = ", ".join([f"'{p}'" for p in prediction_job_ids])

        query = f"""
            SELECT p.id AS prediction_id, qs.quantiles
            FROM quantile_sets AS qs
            JOIN (
                predictions_quantile_sets AS pq,
                predictions as p
            )
            WHERE
                p.id = pq.prediction_id AND
                qs.id = pq.quantile_set_id AND
                p.id IN  ({prediction_jobs_ids_str})
            ORDER BY prediction_id
            ;
        """
        result = _DataInterface.get_instance().exec_sql_query(query)

        prediction_job_quantiles = {}

        # get quantiles for every prediction job
        for _, row in result.iterrows():
            pid = row["prediction_id"]

            if pid not in prediction_job_quantiles:
                prediction_job_quantiles[pid] = []

            prediction_job_quantiles[pid] += json.loads(row["quantiles"])

        # add quantiles to prediction job
        for prediction_job in prediction_jobs:
            pid = prediction_job["id"]
            # add quantiles if any
            if pid in prediction_job_quantiles:
                prediction_job["quantiles"] = sorted(prediction_job_quantiles[pid])
                continue
            # add empty list if none (this should not actually happen)
            prediction_job["quantiles"] = []

        return prediction_jobs

    def get_prediction_job(self, pid, model_type=None, is_active=None):
        """Get prediction job for a given pid from the database.

        Args:
            pid (int): Id of prediction job
            model_type (str, optional): Model type. Defaults to None.
            is_active (int, optional): Active job. Defaults to None.

        Returns:
            dict: Prediction job dictionairy with keys:
                id, type, model, horizon_minutes, resolution_minutes, lat, lon, sid,
                created, description, quantiles
        """

        query = self.build_get_prediction_jobs_query(
            pid=pid, model_type=model_type, is_active=is_active
        )
        result = _DataInterface.get_instance().exec_sql_query(query)

        if result.size == 0:
            raise ValueError(f"No prediction job found with id '{pid}'")

        # Convert to dictionary
        prediction_job = result.to_dict(orient="records")[0]

        # Add description
        prediction_job = self._add_description_to_prediction_job(prediction_job)

        # Add quantiles
        prediction_job = self._add_quantiles_to_prediciton_job(prediction_job)

        # Add model group
        prediction_job = self._add_model_type_group_to_prediction_job(prediction_job)

        return prediction_job

    def get_prediction_jobs(self, model_type=None, is_active=1, only_ato=False):
        """Get all prediction jobs from the database.

        Args:
            model_type (str): Only retrieve jobs with this modeltype specified, e.g. 'xgb'.
                if None, all jobs are retrieved
            is_active (int): Only retrieve jobs where active == is_active.
                if None, all jobs are retrieved

        Returns:
            list: List of prediction jobs
        """
        query = self.build_get_prediction_jobs_query(
            model_type=model_type, is_active=is_active, only_ato=only_ato
        )

        # TODO check length results
        results = _DataInterface.get_instance().exec_sql_query(query)

        # Convert to list of dictionaries
        prediction_jobs = results.to_dict(orient="records")

        # Add description to all prediction jobs
        prediction_jobs = self._add_description_to_prediction_jobs(prediction_jobs)

        # Add quantiles
        prediction_jobs = self._add_quantiles_to_prediciton_jobs(prediction_jobs)

        # Add model group
        prediction_jobs = self._add_model_type_group_to_prediction_jobs(prediction_jobs)

        return prediction_jobs

    @staticmethod
    def build_get_prediction_jobs_query(
        pid=None, model_type=None, is_active=None, only_ato=False
    ):
        where_criteria = []

        if pid is not None:
            if isinstance(pid, list):
                prediction_ids = ", ".join([f"'{p}'" for p in pid])
            elif isinstance(pid, int) or isinstance(pid, str):
                prediction_ids = f"'{pid}'"
            else:
                raise ValueError("pid should be int or str or list of int or str")
            where_criteria.append(f"p.id IN ({prediction_ids})")

        if model_type is not None:
            if isinstance(model_type, list):
                model_types = ", ".join([f"'{m}'" for m in model_type])
            elif isinstance(model_type, str):
                model_types = f"'{model_type}'"
            else:
                raise ValueError("model_type should be str or list of str")
            where_criteria.append(f"p.model IN ({model_types})")

        if is_active is not None:
            # make sure we always get 0 or 1 (anything not 0 -> 1)
            is_active = int(is_active != 0)
            where_criteria.append(f"p.active = {is_active}")

        if only_ato:
            where_criteria.append("`name` LIKE 'ATO%' AND `name` NOT LIKE '%HS%'")

        where = ""
        if len(where_criteria) > 0:
            where = f"WHERE {' AND '.join(where_criteria)}"

        query = f"""
            SELECT
                p.id, p.typ, p.model, p.horizon_minutes, p.resolution_minutes,
                p.train_components, p.name,
                min(s.lat) as lat,
                min(s.lon) as lon,
                min(s.sid) as sid,
                p.created as created
            FROM predictions as p
            LEFT JOIN
                predictions_systems as ps ON p.id = ps.prediction_id
            LEFT JOIN
                systems as s ON s.sid = ps.system_id
            {where}
            GROUP BY p.id;
        """
        return query

    def get_prediction_jobs_wind(self):
        query = """
            SELECT
                p.id, p.typ, p.model, p.horizon_minutes, p.resolution_minutes,
                p.name,
                min(s.sid) as sid,
                w.lat as lat,
                w.lon as lon,
                w.turbine_type,
                w.n_turbines,
                w.hub_height
            FROM predictions as p
            LEFT JOIN predictions_systems as ps ON p.id = ps.prediction_id
            LEFT JOIN systems as s ON s.sid = ps.system_id
            LEFT JOIN windspecs as w ON p.id = w.pid
            WHERE p.typ = 'wind' AND p.active = 1
            GROUP BY p.id
        """

        result = _DataInterface.get_instance().exec_sql_query(query)

        # Convert to list of dictionaries
        result = result.to_dict(orient="records")

        # Add description field
        for pj in result:
            systems = Systems().get_systems_by_pid(pj["id"])
            pj["description"] = "+".join(list(systems.system_id))
        return result

    def get_prediction_jobs_solar(self):
        query = """
            SELECT
                p.id, p.typ, p.model, p.horizon_minutes, p.resolution_minutes,
                p.name,
                min(s.lat) as lat,
                min(s.lon) as lon,
                min(s.sid) as sid,
                ss.lat,
                ss.lon,
                ss.radius,
                ss.peak_power
            FROM predictions as p
            LEFT JOIN predictions_systems as ps ON p.id = ps.prediction_id
            LEFT JOIN systems as s ON s.sid = ps.system_id
            LEFT JOIN solarspecs as ss ON p.id = ss.pid
            WHERE p.typ = 'solar' AND p.active = 1
            GROUP BY p.id
        """

        result = _DataInterface.get_instance().exec_sql_query(query)

        # Convert to list of dictionaries
        result = result.to_dict(orient="records")

        # Add description field
        for pj in result:
            systems = Systems().get_systems_by_pid(pj["id"])
            pj["description"] = "+".join(list(systems.system_id))
        return result

    def get_hyper_params_last_optimized(self, pj):
        """Method that finds the date of the most recent hyperparameters
        Args:
            pj: Prediction job (dict).
        Returns:
            (datetime) last: Datetime of last hyperparameters
        """
        query = f""" SELECT MAX(hpv.created) as last
                    FROM hyper_params hp
                    LEFT JOIN hyper_param_values hpv
                        ON hpv.hyper_params_id=hp.id
                    WHERE hpv.prediction_id={pj["id"]} AND hp.model="{pj["model"]}"
                    """
        last = None
        try:
            # Execute query
            result = _DataInterface.get_instance().exec_sql_query(query)
            # Convert result datetime instance
            last = result["last"][0].to_pydatetime()
            # If dictionary is empty raise exception and fall back to defaults
        except Exception as e:
            print(
                "Could not retrieve last hyperparemeters from database for pid {}\n".format(
                    pj["id"]
                ),
                "Exception: ",
                e,
            )
        return last

    def get_hyper_params(self, pj):
        """Method that finds the latest hyperparameters for a specific prediction job.
        Args:
            pj: Prediction job (dict).

        Returns:
            (dict) params: Dictionary with hyperparameters.
                Empty if no hyperparameters excist or in case of errors.
        """
        # Compose query
        query = f"""
            SELECT hp.name, hpv.value
            FROM hyper_params hp
            LEFT JOIN hyper_param_values hpv
                ON hpv.hyper_params_id=hp.id
            WHERE hpv.prediction_id="{pj["id"]}" AND hp.model="{pj["model"]}"
        """
        # Default params is empty dict
        params = {}

        try:
            # Execute query
            result = _DataInterface.get_instance().exec_sql_query(query)
            # Convert result to dict with proper keys
            params = result.set_index("name").to_dict()["value"]
        except Exception as e:
            self.logger.error(
                "Error occured while retrieving hyper parameters",
                exc_info=e,
                pid=pj["id"],
            )

        return params

    def get_featureset(self, featureset_name):

        if featureset_name not in FEATURESET_NAMES:
            raise KeyError(
                f"Unknown featureset name '{featureset_name}'. "
                f"Valid names are {', '.join(FEATURESET_NAMES)}"
            )

        return FEATURESETS[featureset_name]

    def get_featuresets(self):
        return FEATURESETS

    def get_featureset_names(self):
        return FEATURESET_NAMES
