# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import warnings
from json.decoder import JSONDecodeError

import pandas as pd
import pytz
import requests
import time

from openstef_dbc.log import logging


class ApiException(Exception):
    pass


class KtpApi:
    """Used for all api traffic to Influx API"""

    __INFLUX_TEST_URL = "api/_core/health/liveness"
    __MEASUREMENTS_URL = "api/measurements"
    __SYSTEMS_URL = "admin/systems"
    __TRACY_JOB_URL = "admin/tracyjobs"

    __ALL_TRACY_JOBS = -999

    def __init__(
        self,
        username,
        password,
        url,
        admin_username=None,
        admin_password=None,
        proxies=None,
        timeout=None,
    ):
        """Define api class"""
        # get config
        self.logger = logging.get_logger(self.__class__.__name__)

        self.base_url = url
        self.credentials = username, password
        self.admin_credentials = admin_username, admin_password
        self.proxies = proxies

        self.timeout = timeout
        # Test if the connection works
        self.test()

    def _get_credentials(self, path):
        """
        Returns the admin or api credentials, depending on the path

        :param path: URL to access
        :return: admin or api credentials
        """
        if "admin" in path:
            return self.admin_credentials
        else:
            return self.credentials

    def _get(self, path, params=None):
        """Preform a GET request on the API.

        Args:
            path: the URL to get the data from
            params: the query params.

        Raises: Warning if the response status != 200

        Returns:
            requests.models.Response object
        """

        endpoint = requests.compat.urljoin(self.base_url, path)
        credentials = self._get_credentials(path)

        r = requests.get(
            endpoint,
            auth=credentials,
            proxies=self.proxies,
            params=params,
            timeout=self.timeout,
        )
        if r.status_code != 200:
            warnings.warn(
                "GET for url {} failed with status {} and message: {}".format(
                    endpoint, r.status_code, r.text
                )
            )

        return r

    def _post(self, path, payload, params=None):
        """
        Issue a POST to the given path with the given payload
        :param path: URL to post to
        :param payload: the POST payload
        :return: `Response <Response>` object
        """
        # TODO: Why not have _post raise an exception when it fails
        # (Note: need to change code which checks for the success string)
        endpoint = requests.compat.urljoin(self.base_url, path)
        credentials = self._get_credentials(path)

        p = requests.post(
            endpoint,
            json=payload,
            auth=credentials,
            proxies=self.proxies,
            params=params,
            timeout=self.timeout,
        )
        return p

    def _patch(self, path, payload):
        """
        Issue a PATCH request to the given path with the given payload

        :param path:  URL to PATCH the payload to
        :param payload: payload with fields to update
        :return: updated element
        """

        endpoint = requests.compat.urljoin(self.base_url, path)
        credentials = self._get_credentials(path)

        p = requests.patch(
            endpoint,
            json=payload,
            auth=credentials,
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if p.status_code != 200:
            warnings.warn(
                "PATCH for url {} with payload {} failed: {}".format(
                    endpoint, payload, p.text
                )
            )

        return p.json()

    def _put(self, path, payload):
        endpoint = requests.compat.urljoin(self.base_url, path)
        credentials = self._get_credentials(path)

        u = requests.put(
            endpoint,
            json=payload,
            auth=credentials,
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if u.status_code != 200:
            warnings.warn(
                "PUT for url {} with payload {} failed: {}".format(
                    endpoint, payload, u.text
                )
            )

        return u.json()

    def _delete(self, path):
        endpoint = requests.compat.urljoin(self.base_url, path)
        credentials = self._get_credentials(path)

        d = requests.delete(
            endpoint, auth=credentials, proxies=self.proxies, timeout=self.timeout
        )
        if d.status_code != 204:
            warnings.warn("DELETE for url {} failed: {}".format(endpoint, d.text))

    def test(self):
        """Test connection by pinging the influx database"""
        try:
            r = self._get(KtpApi.__INFLUX_TEST_URL)
        except Exception as e:
            warnings.warn(
                f"Could not establish connection with ktp-api. Continuing anyway... {e}"
            )
            r = dict(status=200)
        return r

    def get_customers(self):
        """
        :return: dataframe with all customers
        """
        r = self._get("admin/customers")

        customers = pd.DataFrame(r.json())
        return customers

    # NOTE this is the new implementation
    def post_measurement(self, measurement, api_key):
        measurements_url = KtpApi.__MEASUREMENTS_URL

        payload = measurement.to_dict()

        params = {"key": api_key}

        try:
            r = self._post(measurements_url, payload, params)
        except Exception as e:
            raise self._build_api_exception_from_requests_exception(e)

        if r.status_code != 201:
            raise self._build_api_exception_from_response(r)

        response = r.json()

        return response

    # NOTE this one will be combined and depreciated in the future
    def post_measurements(self, data, sid, api_key):
        """
        This function can be used to post measured data to our Java API.

        :param data: pd.DataFrame(index = datetime, columns = [output])
        :param sid: str SystemId
        :param api_key: the API key
        :return: string with either a message, or the return value of the POST request
        """

        # API set-up
        measurement_url = KtpApi.__MEASUREMENTS_URL + "/?key={0}".format(api_key)

        ####
        # Change format of data
        ds = data.reset_index()
        # Check if a timezone is present, else add utc
        if ds["datetime"][0].tzinfo is None:
            self.logger.warning("Adding timezone, assuming UTC")
            ds["datetime"] = [x.replace(tzinfo=pytz.UTC) for x in ds["datetime"]]
        ds.set_index("datetime").tz_convert("CET").reset_index()
        # Convert time to isoformat
        ds["datetime"] = ds["datetime"].apply(
            lambda x: x.tz_convert("CET")
            .isoformat()
            .replace("+00:00", "+0000")
            .replace("+01:00", "+0100")
            .replace("+02:00", "+0200")
        )

        payload = dict(sid=sid, data=ds.to_dict("records"))

        p = self._post(measurement_url, payload)

        if p.ok:
            message = "Post successful! Entries: " + str(len(p.json()["data"]))
        else:
            message = p.json()
        return message

    def get_system(self, sid):
        systems_url = f"{KtpApi.__SYSTEMS_URL}/{sid}"
        params = {}

        try:
            r = self._get(systems_url, params)
        except Exception as e:
            raise self._build_api_exception_from_requests_exception(e)

        if r.status_code != 200:
            raise self._build_api_exception_from_response(r)

        system = r.json()

        return system

    def get_systems(self, origin=None, limit=None):
        """Get systems.

        Args:
            origin (str, optional): System origin. Defaults to None.
            limit (int, optional): Limit the number of systems. Defaults to None.

        Raises:
            ApiException

        Returns:
            list: Raw API response systems, list of dicts.
        """
        systems_url = KtpApi.__SYSTEMS_URL
        params = {}

        if origin is not None:
            params["origin"] = origin.upper()
        if limit is not None:
            params["limit"] = limit

        try:
            r = self._get(systems_url, params)
        except Exception as e:
            raise self._build_api_exception_from_requests_exception(e)

        if r.status_code != 200:
            raise self._build_api_exception_from_response(r)

        systems = r.json()

        return systems

    def update_system(self, sid, fields):
        """
        Updates the fields for the given system

        :param sid: system ID
        :param fields:  fields to update
        :return: updated system
        """
        systems_url = KtpApi.__SYSTEMS_URL + "/{0}".format(sid)
        return self._patch(systems_url, fields)

    def _tracy_function_tolower(self, jobs):
        if isinstance(jobs, list):
            for job in jobs:
                job["function"] = job["function"].lower()
        else:
            jobs["function"] = jobs["function"].lower()
        return jobs

    def get_all_tracy_jobs(self, inprogress=__ALL_TRACY_JOBS):
        """
        Retrieves all Tracy Jobs from the Influx API. If inprogress is 0, all entries from the database will be returned
        that either have inprogress to 0 or to NULL

        :param inprogress: filter on inprogress. If no filter is given, all Tracy Jobs are returned.
                           inprogress must be between 0 and 2
        :return: a list with all Tracy Jobs
        """
        r = self._get(KtpApi.__TRACY_JOB_URL)

        # add a quick and dirty retry.
        max_retries = 3
        while max_retries & (r.status_code != 200):
            r = self._get(KtpApi.__TRACY_JOB_URL)
            time.sleep(2)

        # Finally
        if r.status_code != 200:
            # Raise exception of no valid response.
            # Alternatively, we could chose to return empty job list.
            # Lets try this first
            raise ConnectionError(
                "API endpoint to retrieve tracy jobs was not available"
            )
        jobs = self._tracy_function_tolower(r.json())

        # No filter
        if inprogress == KtpApi.__ALL_TRACY_JOBS:
            return jobs

        return list(filter(lambda x: (x.get("inprogress", 0) == inprogress), jobs))

    def get_tracy_job(self, pid, function="train_model"):
        """
        Return the tracy job from the database

        :param pid:
        :param function:
        :return: return the tracy job if it exsits or None if it doesn't
        """
        jobs = self.get_all_tracy_jobs()

        try:
            return next(
                item
                for item in jobs
                if item["args"] == str(pid) and item["function"].lower() == function
            )
        except StopIteration:
            pass

        return None

    def add_tracy_job(self, pid, function="train_model"):
        """
        Adds the function to the tracy todo list

        :param pid: args of the job
        :param function: function to execute
        :return: The Tracy Job
        """
        r = self._post(
            KtpApi.__TRACY_JOB_URL, {"args": str(pid), "function": function.upper()}
        )

        # Check status code. 200 is OK. 400 and 409 might be returned if the job already exists which is also fine
        if r.status_code == 200:
            return self._tracy_function_tolower(r.json())
        elif r.status_code == 409:
            # Already exists. Return existing job
            return self.get_tracy_job(pid, function)
        else:
            warnings.warn(
                "Unable to add Tracy Job. Status code: {0}. Message: {1}".format(
                    r.status_code, r.text
                )
            )

    def update_tracy_job(self, job):
        """
        Update the given Tracy Job by setting inprogess to the requested value

        :param job: prediction job to update
        :return: updated tracy job
        """

        job["function"] = job["function"].upper()
        self._put(KtpApi.__TRACY_JOB_URL + "/{0}".format(job["id"]), job)
        job = self._tracy_function_tolower(job)
        return job

    def delete_tracy_job(self, job):
        """
        Deletes the tracy job

        :param job: job to delete
        :return: -
        """
        return self._delete(KtpApi.__TRACY_JOB_URL + "/{0}".format(job["id"]))

    def _build_api_exception_from_response(self, response):
        code = response.status_code
        method = response.request.method
        path_url = response.request.path_url
        # API specific info
        try:
            r = response.json()
            api_error = r["error"]
            api_message = r["message"]
            error_description = f"error: {api_error}, message: {api_message}"
        except JSONDecodeError:
            error_description = response.text

        return ApiException(
            f"API non 200 status code: [{code}] [{method}] [{path_url}]. "
            f"{error_description}"
        )

    def _build_api_exception_from_requests_exception(self, e):
        return ApiException(f"An exception occured while making an API requests: {e}")
