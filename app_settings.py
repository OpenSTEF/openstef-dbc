"""App and API settings."""
import os
from typing import Union

from pydantic import BaseSettings, Field


class AppSettings(BaseSettings):
    """Global app and API settings.

    Define your default values here.
    Values can be overriden by ENV variables with the same name (case-insensitive).

    If you want all ENV variables to have a certain prefix and still be parsed into
    these AppSettings, you can set the `env_prefix` variable below.

    For example: if `env_prefix = "KTP_"`,
    then the env variable `KTP_LOG_LEVEL` will be parsed to `AppSettings.log_level`.

    Check the docs for more information: https://pydantic-docs.helpmanual.io/usage/settings/

    In the code, you can access these variables like this:
        >>> from app.core.settings import Settings
        >>> print(Settings.app_name)
        template-kubernetes-fastapi
    """

    # DEPLOYMENT and ENVIRONMENT Settings
    loglevel: str = "INFO"

    class Config:
        # Set a prefix to all ENV variables.
        env_prefix = ""
        env_file = ".env"  # .env file should be at base of repo

    # Config map
    teams_alert_url: str = Field(
        os.environ.get("KTP_TEAMS_ALERT_URL"), description="Teams alert url."
    )
    teams_monitoring_url: str = Field(
        os.environ.get("KTP_TEAMS_MONITORING_URL"), description="Teams monitoring url."
    )

    apx_host: str = Field("", description="APX host.")
    apx_port: str = Field("22", description="APX port.")
    apx_username: str = Field(
        os.environ.get("KTP_APX_USERNAME"), description="APX username."
    )
    apx_password: str = Field(
        os.environ.get("KTP_APX_PASSWORD"), description="APX password."
    )

    knmi_api_url: str = Field("", description="")
    knmi_dataset_name: str = Field(
        "harmonie_arome_cy40_p1", description="KNMI Dataset name."
    )
    knmi_dataset_version: str = Field("0.2", description="KNMI dataset version.")
    knmi_api_key: str = Field(
        os.environ.get("KTP_KNMI_API_KEY"), description="KNMI API key."
    )

    pvoutput_getsystem_endpoint_url: str = Field("", description="")
    pvoutput_getregionstatus_endpoint_url: str = Field("", description="")
    pvoutput_api_sid: str = Field(
        os.environ.get("KTP_PVOUTPUT_API_SID"), description="PVOUTPUT API sid."
    )
    pvoutput_api_key: str = Field(
        os.environ.get("KTP_PVOUTPUT_API_KEY"), description="PVOUTPUT API key."
    )

    api_url: str = Field("", description="")
    api_username: str = Field(
        os.environ.get("KTP_API_USERNAME"), description="API username."
    )
    api_password: str = Field(
        os.environ.get("KTP_API_PASSWORD"), description="API password."
    )
    api_admin_username: str = Field(
        os.environ.get("KTP_API_ADMIN_USERNAME"), description="API admin username."
    )
    api_admin_password: str = Field(
        os.environ.get("KTP_API_ADMIN_PASSWORD"), description="API admin password."
    )

    mysql_host: str = Field("", description="MySQL host.")
    mysql_port: str = Field("3306", description="MySQL port.")
    mysql_database_name: str = Field("tst_icarus", description="MySQL database name.")
    mysql_username: str = Field(
        os.environ.get("KTP_MYSQL_USERNAME"), description="MySQL username."
    )
    mysql_password: str = Field(
        os.environ.get("KTP_MYSQL_PASSWORD"), description="MySQL password."
    )

    influxdb_host: str = Field("", description="InfluxDB host.")
    influxdb_port: str = Field("8086", description="InfluxDB port.")
    influxdb_username: str = Field(
        os.environ.get("KTP_INFLUXDB_USERNAME"), description="InfluxDB username."
    )
    influxdb_password: str = Field(
        os.environ.get("KTP_INFLUXDB_PASSWORD"), description="InfluxDB password."
    )

    proxies: Union[dict[str, str], None] = None

    env: str = Field("container", description="Environment (local or container)")
