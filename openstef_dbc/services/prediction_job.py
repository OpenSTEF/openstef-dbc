# SPDX-FileCopyrightText: 2021 2017-2021 Contributors to the OpenSTF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import json
from typing import List, Optional, Union
from pydantic import ValidationError
from openstef.data_classes.prediction_job import PredictionJobDataClass

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc.log import logging
from openstef_dbc.services.systems import Systems


class PredictionJobRetriever:
    def __init__(self):
        self.logger = logging.get_logger(self.__class__.__name__)

    def get_prediction_job(
        self, pid: int, model_type: str = None, is_active: int = None
    ) -> PredictionJobDataClass:
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
        prediction_job_dict = result.to_dict("records")[0]

        # Add description
        prediction_job_dict = self._add_description_to_prediction_job(
            prediction_job_dict
        )

        # Add quantiles
        prediction_job_dict = self._add_quantiles_to_prediction_job(prediction_job_dict)

        prediction_job = self._create_prediction_job_object(prediction_job_dict)
        return prediction_job

    def get_prediction_jobs(
        self,
        model_type: Optional[str] = None,
        is_active: int = 1,
        only_ato: bool = False,
        limit: Optional[int] = None,
    ) -> List[PredictionJobDataClass]:
        """Get all prediction jobs from the database.

        Args:
            model_type (str): Only retrieve jobs with specified modeltype (e.g. 'xgb').
                if None, all jobs are retrieved
            is_active (int): Only retrieve jobs where active == is_active.
                if None, all jobs are retrieved
            only_ato (bool): Only retrieve ATO jobs
            limit (int): Limit the number of jobs to given value.

        Returns:
            List[dict]: List of prediction jobs
        """
        query = self.build_get_prediction_jobs_query(
            model_type=model_type,
            is_active=is_active,
            only_ato=only_ato,
            limit=limit,
        )

        # Retrieve prediction jobs from database as a list of dictionaries
        prediction_jobs = self._get_prediction_jobs_query_results(query)

        # Add quantiles
        prediction_jobs = self._add_quantiles_to_prediction_jobs(prediction_jobs)

        # Change prediction jobs to dataclass
        prediction_jobs = [
            self._create_prediction_job_object(prediction_job)
            for prediction_job in prediction_jobs
        ]

        return prediction_jobs

    def get_prediction_jobs_wind(self):
        query = """
            SELECT
                p.id, 
                p.forecast_type, 
                p.model, 
                p.horizon_minutes, 
                p.resolution_minutes,
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
            WHERE p.forecast_type = 'wind' AND p.active = 1
            GROUP BY p.id
        """

        # Retrieve prediction jobs from database
        prediction_jobs = self._get_prediction_jobs_query_results(query)

        return prediction_jobs

    def get_prediction_jobs_solar(self):
        query = """
            SELECT
                p.id, 
                p.forecast_type, 
                p.model, 
                p.horizon_minutes, 
                p.resolution_minutes,
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
            WHERE p.forecast_type = 'solar' AND p.active = 1
            GROUP BY p.id
        """

        # Retrieve prediction jobs from database
        prediction_jobs = self._get_prediction_jobs_query_results(query)

        return prediction_jobs

    def _get_prediction_jobs_query_results(
        self, query: str
    ) -> List[PredictionJobDataClass]:
        """Get prediction jobs using a query to the database

         Args:
             query (str): the sql query to use on the database

        Returns:
            prediction_jobs (list): List of prediction job dictionaries
        """
        results = _DataInterface.get_instance().exec_sql_query(query)
        if len(results) == 0:
            return []

        # Convert to list of dictionaries
        prediction_jobs = results.to_dict("records")

        # Add description to all prediction jobs
        prediction_jobs = self._add_description_to_prediction_jobs(prediction_jobs)

        return prediction_jobs

    def _create_prediction_job_object(
        self, pj: PredictionJobDataClass
    ) -> PredictionJobDataClass:
        """Create an object for the prediction job from a dictionary

        Args:
            pj (dict): dictionary with the attributes from the prediction job

        Returns:
            prediction_job_object (object): data class of the prediction job
        """
        try:
            # Change the typ column to forecast_type
            if "typ" in pj:
                pj["forecast_type"] = pj.pop("typ")
            prediction_job_object = PredictionJobDataClass(**pj)
        except ValidationError as e:
            errors = e.errors()
            self.logger.error(
                "Error occurred while converting to data class",
                pid=pj["id"],
                error=errors,
            )
            raise AttributeError(e)
        return prediction_job_object

    def _add_description_to_prediction_job(
        self, prediction_job: PredictionJobDataClass
    ) -> PredictionJobDataClass:
        return self._add_description_to_prediction_jobs([prediction_job])[0]

    def _add_quantiles_to_prediction_job(
        self, prediction_job: PredictionJobDataClass
    ) -> PredictionJobDataClass:
        return self._add_quantiles_to_prediction_jobs([prediction_job])[0]

    @classmethod
    def _add_quantiles_to_prediction_jobs(
        cls, prediction_jobs: List[PredictionJobDataClass]
    ) -> List[PredictionJobDataClass]:
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

    @classmethod
    def build_get_prediction_jobs_query(
        cls,
        pid: Union[int, str, List[int], List[str], None] = None,
        model_type: Union[str, List[str], None] = None,
        is_active: Optional[int] = None,
        only_ato: bool = False,
        limit: Optional[int] = None,
    ) -> str:
        where_condition = []

        if pid is not None:
            where_condition.append(
                PredictionJobRetriever._build_pid_where_condition(pid)
            )

        if model_type is not None:
            where_condition.append(
                PredictionJobRetriever._build_model_type_where_condition(model_type)
            )

        if is_active is not None:
            where_condition.append(
                PredictionJobRetriever._build_active_where_condition(is_active)
            )

        if only_ato:
            where_condition.append("`name` LIKE 'ATO%' AND `name` NOT LIKE '%HS%'")

        where_clause = ""
        limit_clause = ""

        if len(where_condition) > 0:
            where_clause = f"WHERE {' AND '.join(where_condition)}"

        if limit:
            limit_clause = f"LIMIT {limit}"

        query = f"""
            SELECT
                p.id, p.name,
                p.forecast_type, p.model, p.horizon_minutes, p.resolution_minutes,
                p.train_components,
                min(s.lat) as lat,
                min(s.lon) as lon
            FROM predictions as p
            LEFT JOIN
                predictions_systems as ps ON p.id = ps.prediction_id
            LEFT JOIN
                systems as s ON s.sid = ps.system_id
            {where_clause}
            GROUP BY p.id
            {limit_clause};
        """
        return query

    @staticmethod
    def _add_description_to_prediction_jobs(
        prediction_jobs: List[PredictionJobDataClass],
    ) -> List[PredictionJobDataClass]:
        for prediction_job in prediction_jobs:
            systems = Systems().get_systems_by_pid(
                pid=prediction_job["id"], return_list=True
            )
            systems_str = "+".join([s["system_id"] for s in systems])
            prediction_job["description"] = systems_str

        return prediction_jobs

    @staticmethod
    def _build_pid_where_condition(pid: int) -> str:
        if isinstance(pid, list):
            in_values = ", ".join([f"'{p}'" for p in pid])
        elif isinstance(pid, int) or isinstance(pid, str):
            in_values = f"'{pid}'"
        else:
            raise ValueError("pid should be int, str, list of int or list of str")
        return f"p.id IN ({in_values})"

    @staticmethod
    def _build_model_type_where_condition(model_type: Union[list, str]) -> str:
        if isinstance(model_type, list):
            in_values = ", ".join([f"'{v}'" for v in model_type])
        elif isinstance(model_type, str):
            in_values = f"'{model_type}'"
        else:
            raise ValueError("model_type should be str or list of str")
        return f"p.model IN ({in_values})"

    @staticmethod
    def _build_active_where_condition(active: int) -> str:
        # make sure we always get 0 or 1 (anything not 0 -> 1)
        active = int(active != 0)
        return f"p.active = {active}"
