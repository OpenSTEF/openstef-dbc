# SPDX-FileCopyrightText: 2017-2023 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com> # noqa E501>
#
# SPDX-License-Identifier: MPL-2.0
from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Global app settings."""

    model_config = SettingsConfigDict(
        env_prefix="openstef_", env_file=".env", extra="ignore"
    )

    optimum_weather_sources: List[str] = Field(
        [
            "harm_arome",
            "harm_arome_fallback",
            "GFS_50",
            "harmonie",
            "icon",
            "DSN",
            "ECMWF_HRES",
            "METEO_FRANCE_ARPEGE",
        ],
        description="List of weather sources to use for fetching weather data from influx using openstef DBC.",
    )


@lru_cache
def _get_app_settings() -> AppSettings:
    return AppSettings()


Settings = _get_app_settings()
