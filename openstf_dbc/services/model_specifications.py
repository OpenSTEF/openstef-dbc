# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime
from pydantic import BaseModel
from typing import Dict, Optional, Union

from openstf_dbc.data.featuresets import FEATURESET_NAMES, FEATURESETS
from openstf_dbc.data_interface import _DataInterface
from openstf_dbc.log import logging


class ModelSpecificationDataClass(BaseModel):
    id: Union[int, str]
    hyper_params: Optional[dict]
    feature_names: Optional[list]


class ModelSpecificationRetriever:
    def __init__(self):
        self.logger = logging.get_logger(self.__class__.__name__)
        # If the ModelSpecificationDataClass gets expanded or needs to be validated,
        # we can add get_model_specfications and use ModelSpecificationDataClass

    def get_hyper_params(self, pj: dict) -> Dict[str, float]:
        """Find the latest hyperparameters for a specific prediction job."""
        query = f"""
            SELECT 
                hp.name, 
                hpv.value
            FROM hyper_params hp
            LEFT JOIN hyper_param_values hpv
            ON hpv.hyper_params_id=hp.id
            WHERE 
                hpv.prediction_id="{pj["id"]}" AND 
                hp.model="{pj["model"]}"
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

    def get_hyper_params_last_optimized(self, pj: dict) -> list[datetime]:
        """Find the date of the most recent hyperparameters."""
        query = f"""
            SELECT MAX(hpv.created) as last
            FROM hyper_params hp
            LEFT JOIN hyper_param_values hpv
            ON hpv.hyper_params_id = hp.id
            WHERE 
                hpv.prediction_id={pj["id"]} AND 
                hp.model="{pj["model"]}"
        """
        last = None
        try:
            # Execute query
            result = _DataInterface.get_instance().exec_sql_query(query)
            # Convert result datetime instance
            last = result["last"][0].to_pydatetime()
            # If dictionary is empty raise exception and fall back to defaults
        except Exception as e:
            self.logger.error(
                "Could not retrieve last hyperparameters from database ",
                pid=pj["id"],
                exc_info=e,
            )

        return last

    @staticmethod
    def get_featureset(featureset_name: str) -> list[str]:
        """Give predefined featureset based on input featureset_name."""
        if featureset_name not in FEATURESET_NAMES:
            raise KeyError(
                f"Unknown featureset name '{featureset_name}'. "
                f"Valid names are {', '.join(FEATURESET_NAMES)}"
            )
        return FEATURESETS[featureset_name]