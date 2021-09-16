# SPDX-FileCopyrightText: 2021 2017-2021 Alliander N.V. <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0

import unittest
import warnings
from datetime import datetime
from unittest import TestCase, mock
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytz
from openstf_dbc.ktp_api import KtpApi
from pytz import timezone

customers_list = [
    {
        "id": 300,
        "name": "Flex_ZPP",
        "active": True,
        "created": datetime.utcnow().isoformat(),
    },
    {
        "id": 301,
        "name": "Flex_WLS",
        "active": True,
        "created": datetime.utcnow().isoformat(),
    },
]

tracy_jobs_list = [
    {"id": 1, "args": "arguments", "function": "TRAIN_MODEL"},
    {
        "id": 2,
        "args": "arguments2",
        "function": "OPTIMIZE_HYPERPARAMETERS",
        "inprogress": 2,
    },
    {
        "id": 3,
        "args": "arguments2",
        "function": "OPTIMIZE_HYPERPARAMETERS",
    },
]

measurements = [
    {"datetime": "2019-10-16 20:00:00+00:00", "output": -0.0},
    {"datetime": "2019-10-16 20:15:00+00:00", "output": -1.0},
    {"datetime": "2019-10-16 20:30:00+00:00", "output": -3.1},
]

measurements_no_tz = [
    {"datetime": "2019-10-16 20:00:00", "output": -0.0},
    {"datetime": "2019-10-16 20:15:00", "output": -1.0},
    {"datetime": "2019-10-16 20:30:00", "output": -3.1},
]


# Load example measurements
measurements_pf = pd.DataFrame(measurements)
measurements_pf["datetime"] = pd.to_datetime(measurements_pf["datetime"])

measurements_no_tz_pf = pd.DataFrame(measurements_no_tz)
measurements_no_tz_pf["datetime"] = pd.to_datetime(measurements_no_tz_pf["datetime"])
error_string = "There was a (mocked) error"


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if "api/influx/ping" in args[0]:
        return MockResponse({"status": "OK"}, 200)

    elif "admin/customers" in args[0]:
        return MockResponse(customers_list, 200)
    elif "admin/tracyjobs" in args[0]:
        return MockResponse(tracy_jobs_list, 200)

    return MockResponse(error_string, 404)


def mocked_requests_get_fail(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.text = json_data

        def json(self):
            return self.json_data

    return MockResponse(error_string, 404)


# def mocked_config(*args, **kwargs):
#     mock_config = MagicMock()
#     mock_config.api.url = "https://api.api"
#     mock_config.api.username = "username"
#     mock_config.api.password = "password"
#     mock_config.api.admin_username = "admin_username"
#     mock_config.api.admin_password = "admin_password"
#     mock_config.proxies = None
#     return mock_config

api_kwargs = {"url": "https://api.api", "username": "username", "password": "password"}


# @mock.patch("openstf_dbc.ktp_api.ConfigManager.get_instance", mocked_config)
@mock.patch("requests.get", side_effect=mocked_requests_get)
class TestKtpApi(TestCase):
    def test_ktp_api(self, mock_get):
        with warnings.catch_warnings(record=True):
            KtpApi(**api_kwargs)

    def test_get_customers(self, mock_get):
        ktp_api = KtpApi(**api_kwargs)
        customers = ktp_api.get_customers()

        # Check the correct URL is used
        assert "admin/customers" in mock_get.call_args[0][0]
        # Check if we received the complete list with customers
        pd.testing.assert_frame_equal(customers, pd.DataFrame(customers_list))

    @mock.patch("requests.post")
    def test_post_measurements_with_tz(self, mock_post, mock_get):
        """Test with a timezone"""

        sampleSID = "sampleSID"
        sampleKey = "sampleKey"
        ktp_api = KtpApi(**api_kwargs)
        ret = ktp_api.post_measurements(measurements_pf, sampleSID, sampleKey)

        full_url = mock_post.call_args[0][0]
        json = mock_post.call_args[1]["json"]

        # Check return value
        assert "successful" in ret
        # Check the correct URL is used
        assert "api/measurements" in full_url
        # With the correct key
        assert "key=" + sampleKey in full_url
        # Check sid
        assert json["sid"] == sampleSID

        # Check datetime is the same
        for i, line in enumerate(json["data"]):
            datetime_post = line["datetime"]
            datetime_orig = measurements[i]["datetime"]

            date_post = datetime.strptime(datetime_post, "%Y-%m-%dT%H:%M:%S%z")
            date_orig = datetime.strptime(datetime_orig, "%Y-%m-%d %H:%M:%S%z")

            assert date_post == date_orig

    @mock.patch("requests.post")
    def test_post_measurements_without_tz(self, mock_post, mock_get):
        """Test without a timezone"""

        sampleSID = "sampleSID"
        sampleKey = "sampleKey"
        ktp_api = KtpApi(**api_kwargs)
        ret = ktp_api.post_measurements(measurements_no_tz_pf, sampleSID, sampleKey)

        full_url = mock_post.call_args[0][0]
        json = mock_post.call_args[1]["json"]

        # Check return value
        assert "successful" in ret
        # Check the correct URL is used
        assert "api/measurements" in full_url
        # With the correct key
        assert "key=" + sampleKey in full_url
        # Check sid
        assert json["sid"] == sampleSID

        # Check datetime is the same
        for i, line in enumerate(json["data"]):
            datetime_post = line["datetime"]
            datetime_orig = measurements_no_tz[i]["datetime"]

            date_post = datetime.strptime(datetime_post, "%Y-%m-%dT%H:%M:%S%z")
            # Assume UTC is not timezone is given
            date_orig = pytz.utc.localize(
                datetime.strptime(datetime_orig, "%Y-%m-%d %H:%M:%S")
            )
            assert date_post == date_orig.astimezone(timezone("Europe/Amsterdam"))

    @mock.patch("requests.post")
    def test_post_measurement_fail(self, mock_post, mock_get):
        post_ret = Mock()
        post_ret.ok = False
        post_ret.json.return_value = error_string
        mock_post.return_value = post_ret

        ktp_api = KtpApi(**api_kwargs)
        ret = ktp_api.post_measurements(measurements_no_tz_pf, "sampleSID", "sampleKey")
        assert ret == error_string

    @mock.patch("requests.patch")
    def test_update_systems_ok(self, mock_patch, mock_get):
        ret_string = "Updated System"
        patch_ret = Mock()
        patch_ret.status_code = 200
        patch_ret.json.return_value = ret_string
        mock_patch.return_value = patch_ret
        ktp_api = KtpApi(**api_kwargs)
        sid = "Zwo_150_TR4_P"
        fields = {"lat": 99, "lon": -1, "brand": "beste"}
        r = ktp_api.update_system(sid, fields)
        assert r == ret_string

        url = mock_patch.call_args[0][0]
        assert "admin/systems" in url
        assert sid in url

        payload = mock_patch.call_args[1]["json"]
        for field in fields:
            assert field in payload

    @mock.patch("requests.patch")
    def test_update_systems_fail(self, mock_patch, mock_get):
        ret_string = "Failure_String"
        patch_ret = Mock()
        patch_ret.status_code = 500
        patch_ret.json.return_value = ret_string
        mock_patch.return_value = patch_ret

        with warnings.catch_warnings(record=True):
            KtpApi(**api_kwargs)

    @mock.patch("requests.post")
    def test_add_tracy_job(self, mock_post, mock_get):
        id = tracy_jobs_list[0]["id"]
        function = tracy_jobs_list[0][
            "function"
        ].lower()  # SQL returns uppercase, python uses lower case
        arguments = tracy_jobs_list[0]["args"]

        post_ret = Mock()
        post_ret.json.return_value = {"id": id, "args": arguments, "function": function}
        post_ret.status_code = 200
        mock_post.return_value = post_ret

        with warnings.catch_warnings(record=True):
            ktp_api = KtpApi(**api_kwargs)
            # New Tracy Job

            job = ktp_api.add_tracy_job(arguments, function)

            self.assertEqual(job["id"], id)
            self.assertEqual(job["function"], function)
            self.assertEqual(job["args"], arguments)

            # Existing Tracy Job
            post_ret.status_code = 409
            job = ktp_api.add_tracy_job(arguments, function)

            self.assertEqual(job["id"], id)
            self.assertEqual(job["function"], function)
            self.assertEqual(job["args"], arguments)

            # Error in Tracy Job
            post_ret.status_code = 500

    def test_get_all_tracy_jobs(self, mock_get):
        ktp_api = KtpApi(**api_kwargs)

        # Get all jobs
        jobs = ktp_api.get_all_tracy_jobs()
        self.assertEqual(jobs, tracy_jobs_list)

        # Get specific job with filter
        jobs = ktp_api.get_all_tracy_jobs(inprogress=2)
        self.assertEqual(jobs[0], tracy_jobs_list[1])

        # Get no jobs
        jobs = ktp_api.get_all_tracy_jobs(inprogress=12345)
        self.assertEqual(len(jobs), 0)

    def test_get_tracy_job(self, mock_get):
        id = tracy_jobs_list[1]["id"]
        function = tracy_jobs_list[1][
            "function"
        ].lower()  # SQL returns uppercase, python uses lower case
        arguments = tracy_jobs_list[1]["args"]

        ktp_api = KtpApi(**api_kwargs)

        # Existing Job
        job = ktp_api.get_tracy_job(arguments, function)
        self.assertEqual(job["id"], id)
        self.assertEqual(job["args"], arguments)
        self.assertEqual(job["function"], function)

        # Job does not exist
        job = ktp_api.get_tracy_job("MijnPID3", "optimize_hyperparameters")
        self.assertIsNone(job)

        # Job with default function
        job = ktp_api.get_tracy_job(tracy_jobs_list[0]["args"])
        self.assertEqual(job["id"], tracy_jobs_list[0]["id"])
        self.assertEqual(job["args"], tracy_jobs_list[0]["args"])
        self.assertEqual(job["function"], tracy_jobs_list[0]["function"].lower())

    @mock.patch("requests.put")
    def test_update_tracy_job(self, mock_put, mock_get):
        t_id = tracy_jobs_list[0]["id"]
        arguments = tracy_jobs_list[0]["args"]
        function = tracy_jobs_list[0]["function"]
        inprogess = 2

        put_ret = Mock()
        put_ret.json.return_value = {
            "id": t_id,
            "args": arguments,
            "function": function,
            "inprogress": inprogess,
        }
        put_ret.status_code = 200
        mock_put.return_value = put_ret

        with warnings.catch_warnings(record=True):
            ktp_api = KtpApi(**api_kwargs)
            ktp_api.update_tracy_job(
                {
                    "id": id,
                    "args": arguments,
                    "function": function.lower(),
                    "inprogress": inprogess,
                }
            )

    @mock.patch("requests.put")
    def test_update_tracy_job_fail(self, mock_put, mock_get):
        put_ret = Mock()
        put_ret.status_code = 500
        mock_put.return_value = put_ret

        with warnings.catch_warnings(record=True):
            KtpApi(**api_kwargs)

    @mock.patch("requests.delete")
    def test_delete_tracy_job(self, mock_delete, mock_get):
        put_del = Mock()
        put_del.status_code = 204
        mock_delete.return_value = put_del

        with warnings.catch_warnings(record=True):
            ktp_api = KtpApi(**api_kwargs)
            ktp_api.delete_tracy_job({"id": 4})


if __name__ == "__main__":
    unittest.main()
