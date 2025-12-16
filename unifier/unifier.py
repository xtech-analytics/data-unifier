import logging
import os
import subprocess
import shutil
from typing import Any, Dict, List, Optional, Sequence

import requests
import pandas as pd

logger = logging.getLogger(__name__)

class Unifier:
    """Client helpers to interact with the Unifier API.

    Notes
    -----
    - Credentials can be set via the class variables `user` and `token` or
      passed per-call to the query helpers.
    - The `as_of` parameter is deprecated; prefer `asof_date` and
      `asof_back_to`.
    """
    
    url = 'https://unifier.exponential-tech.ai/unifier'
    user = ''
    token = ''

    __version__ = '0.1.14'

    @classmethod
    def get_asof_dates_query(cls, name: str) -> List[Dict[str, Any]]:
        """Get a list of available as-of dates for a given data `name`.

        Parameters
        ----------
        name: str
            The dataset or query name for which to fetch as-of dates.

        Returns
        -------
        list[dict]
            A flattened list of records with available as-of dates. Returns an
            empty list on error.
        """
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": name,
            "user": cls.user,
            "token": cls.token,
        }

        _url = cls.url + "/get_asof_date"
        try:
            response = requests.post(_url, headers=headers, json=payload)
            if response.status_code != 200:
                # Try to surface server-provided error, otherwise log status code.
                try:
                    err = response.json()
                except ValueError:
                    err = {}
                if isinstance(err, dict) and "error" in err:
                    logger.warning("Unifier as-of dates error: %s", err["error"])
                    return []

                logger.warning(
                    "Unifier as-of dates request failed with status code %s",
                    response.status_code,
                )
                return []

            response_data = response.json()
            if not isinstance(response_data, list):
                logger.warning("Unexpected as-of dates response format: %r", type(response_data))
                return []
            extracted_data = [
                {k: v for d in item for k, v in d.items()} for item in response_data
            ]
            return extracted_data
        except requests.exceptions.RequestException as e:
            logger.error("As-of dates request failed: %s", e)
            return []

    @classmethod
    def get_asof_dates(cls, name: str) -> pd.DataFrame:
        """Get a pandas DataFrame of available as-of dates for a given name."""
        _data = cls.get_asof_dates_query(name)
        return pd.DataFrame(_data)

    @classmethod
    def get_asof_dates_json(cls, name: str) -> List[Dict[str, Any]]:
        """Get a Python list of available as-of dates for a given name."""
        return cls.get_asof_dates_query(name)

    @classmethod
    def query(
        cls,
        name: str,
        user: Optional[str] = None,
        token: Optional[str] = None,
        key: Optional[str] = None,
        keys: Optional[Sequence[str]] = None,
        as_of: Optional[str] = None,
        back_to: Optional[str] = None,
        up_to: Optional[str] = None,
        asof_date: Optional[str] = None,
        asof_back_to: Optional[str] = None,
        limit: Optional[int] = None,
        column_filters: Optional[str] = None,
        disable_view: bool = False,
    ) -> List[Sequence[Dict[str, Any]]]:
        """Query the Unifier API and return the raw response data as-is.

        Parameters
        ----------
        name : str
            The query or dataset name.
        user, token : Optional[str]
            Override class-level credentials.
        key : Optional[str]
            Single key filter.
        keys : Optional[Sequence[str]]
            Multiple keys filter.
        as_of : Optional[str]
            Deprecated; prefer `asof_date` or `asof_back_to`.
        back_to, up_to, asof_date, asof_back_to : Optional[str]
            Temporal filtering parameters.
        limit : Optional[int]
            Max number of rows/records.
        column_filters : Optional[str]
            SQL-compatible boolean expression to filter columns/rows server-side.
        disable_view : bool
            If true, disable view expansion on the server.

        Returns
        -------
        list
            The JSON payload returned by the API (commonly a list of lists of
            dicts). Returns an empty list on error.
        """

        headers = {"Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "name": name,
            "user": cls.user,
            "token": cls.token,
            "disable_view": disable_view,
        }

        if limit is not None:
            payload["limit"] = limit

        if key is not None:
            payload["key"] = key
        if keys is not None:
            payload["keys"] = list(keys)
        if token is not None:
            payload["token"] = token
        if user is not None:
            payload["user"] = user
        if column_filters is not None:
            payload["column_filters"] = column_filters

        # `as_of` is deprecated; map to `asof_date` for backward compatibility.
        if as_of is not None:
            payload["asof_date"] = as_of
        if asof_back_to is not None:
            payload["asof_back_to"] = asof_back_to
        if back_to is not None:
            payload["back_to"] = back_to
        if up_to is not None:
            payload["up_to"] = up_to
        if asof_date is not None:
            payload["asof_date"] = asof_date

        try:
            response = requests.post(cls.url, headers=headers, json=payload)
            if response.status_code != 200:
                try:
                    err = response.json()
                except ValueError:
                    err = {}
                if isinstance(err, dict) and "error" in err:
                    logger.warning("Unifier query error: %s", err["error"])
                    return []

                logger.warning(
                    "Unifier query failed with status code %s", response.status_code
                )
                return []
            response_data = response.json()
            if not isinstance(response_data, list):
                logger.warning("Unexpected query response format: %r", type(response_data))
                return []
            return response_data
        except requests.exceptions.RequestException as e:
            logger.error("Unifier query request failed: %s", e)
            return []

    @classmethod
    def get_dataframe(
        cls,
        name: str,
        user: Optional[str] = None,
        token: Optional[str] = None,
        key: Optional[str] = None,
        keys: Optional[Sequence[str]] = None,
        as_of: Optional[str] = None,
        back_to: Optional[str] = None,
        up_to: Optional[str] = None,
        asof_date: Optional[str] = None,
        asof_back_to: Optional[str] = None,
        limit: Optional[int] = None,
        column_filters: Optional[str] = None,
        disable_view: bool = False,
    ) -> pd.DataFrame:
        """Get the query result as a pandas DataFrame.

        Returns an empty DataFrame on error or empty result.
        """

        json_result = cls.query(
            name,
            user,
            token,
            key,
            keys,
            as_of,
            back_to,
            up_to,
            asof_date,
            asof_back_to,
            limit,
            column_filters,
            disable_view,
        )
        if not json_result:
            return pd.DataFrame()
        extracted_data = [
            {k: v for d in item for k, v in d.items()} for item in json_result
        ]
        return pd.DataFrame(extracted_data)
    
    @classmethod
    def get_json(
        cls,
        name: str,
        user: Optional[str] = None,
        token: Optional[str] = None,
        key: Optional[str] = None,
        keys: Optional[Sequence[str]] = None,
        as_of: Optional[str] = None,
        back_to: Optional[str] = None,
        up_to: Optional[str] = None,
        asof_date: Optional[str] = None,
        asof_back_to: Optional[str] = None,
        limit: Optional[int] = None,
        column_filters: Optional[str] = None,
        disable_view: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get the query result as a flattened JSON list of dicts.

        Returns an empty list on error or empty result.
        """

        json_result = cls.query(
            name,
            user,
            token,
            key,
            keys,
            as_of,
            back_to,
            up_to,
            asof_date,
            asof_back_to,
            limit,
            column_filters,
            disable_view,
        )
        if not json_result:
            return []
        extracted_data = [
            {k: v for d in item for k, v in d.items()} for item in json_result
        ]
        return extracted_data

    @classmethod
    def replicate(
        cls,
        name: str,
        target_location: str,
        asof_date: Optional[str] = None,
        back_to: Optional[str] = None,
        up_to: Optional[str] = None,
        bandwidth_limit: Optional[int] = None,
    ) -> None:
        """Replicate data to a local target location using rclone.

        Parameters
        ----------
        name : str
            The dataset name to replicate.
        target_location : str
            The local directory path where data should be downloaded.
        asof_date : Optional[str]
            The specific date for the data replication (format: YYYY-MM-DD).
        back_to : Optional[str]
            The start date for the data replication (format: YYYY-MM-DD).
        up_to : Optional[str]
            The end date for the data replication (format: YYYY-MM-DD).
        bandwidth_limit : Optional[int]
            The bandwidth limit in MB/s for the replication process.
        """
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": name,
            "user": cls.user,
            "token": cls.token,
        }
        
        if asof_date:
            payload["asof_date"] = asof_date
        if back_to:
            payload["back_to"] = back_to
        if up_to:
            payload["up_to"] = up_to

        if not shutil.which("rclone"):
            logger.error("Rclone is not installed. Please install it to use this feature (https://rclone.org/downloads/).")
            return

        _url = cls.url + "/replicate"

        try:
            # Fetch replication credentials
            response = requests.post(_url, headers=headers, json=payload)
            if response.status_code != 200:
                try:
                    err_msg = response.json().get("error", "Unknown error")
                except ValueError:
                    err_msg = response.text
                logger.warning("Unifier replicate error: %s", err_msg)
                return

            resp_json = response.json()
            data = resp_json.get("data")
            if not data:
                logger.warning("Unifier replicate response missing 'data' field")
                return

            access_key_id = data.get("access_key_id")
            secret_access_key = data.get("secret_access_key")
            data_path = resp_json.get("data_path")
            folders = resp_json.get("folders", [])
            endpoint = resp_json.get("endpoint", "https://s3.wasabisys.com")
            region = resp_json.get("region", "us-east-1")

            if not (access_key_id and secret_access_key and data_path):
                logger.warning("Unifier replicate missing required credentials or path")
                return

            # Prepare rclone environment
            env = os.environ.copy()
            env["RCLONE_CONFIG_UNIFIER_TYPE"] = "s3"
            env["RCLONE_CONFIG_UNIFIER_PROVIDER"] = "Wasabi"
            env["RCLONE_CONFIG_UNIFIER_ENV_AUTH"] = "false"
            env["RCLONE_CONFIG_UNIFIER_ACCESS_KEY_ID"] = access_key_id
            env["RCLONE_CONFIG_UNIFIER_SECRET_ACCESS_KEY"] = secret_access_key
            env["RCLONE_CONFIG_UNIFIER_ENDPOINT"] = endpoint
            env["RCLONE_CONFIG_UNIFIER_REGION"] = region

            # Handle s3:// prefix
            if data_path.startswith("s3://"):
                data_path = data_path[5:]
            elif data_path.startswith("s3a://"):
                data_path = data_path[6:]

            source = f"UNIFIER:{data_path}"
            if not source.endswith("/"):
                source += "/"

            # Construct command
            cmd = ["rclone", "copy", source, target_location, "--progress", "--config", "/dev/null"]

            if bandwidth_limit:
                 cmd.extend(["--bwlimit", f"{bandwidth_limit}M"])

            for folder in folders:
                if folder.startswith("/"):
                    folder = folder.lstrip("/")
                if folder.endswith("/*"):
                    folder = folder[:-1] + "**"
                cmd.extend(["--include", folder])

            logger.info("Starting rclone replication for %s...", name)
            subprocess.run(cmd, env=env, check=True)
            logger.info("Replication completed for %s", name)

        except requests.exceptions.RequestException as e:
            logger.error("Replicate network request failed: %s", e)
        except subprocess.CalledProcessError as e:
            logger.error("Rclone execution failed: %s", e)
        except Exception as e:
            logger.error("An unexpected error occurred during replication: %s", e)
