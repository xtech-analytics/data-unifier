import logging
import os
import subprocess
import shutil
from typing import Any, Dict, List, Optional, Sequence
import fnmatch
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
try:
    import boto3
    from botocore.client import Config
    from boto3.s3.transfer import TransferConfig
except ImportError:
    boto3 = None
    TransferConfig = None

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

    __version__ = '0.1.17'

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
        use_rclone: bool = True,
    ) -> None:
        """Replicate data to a local target location using rclone or native python.

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
            The bandwidth limit in MB/s for the replication process (rclone only).
        use_rclone : bool
            If True, attempt to use rclone. Falls back to native Python implementation
            if rclone is not available.
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

        # Check for rclone availability
        rclone_path = shutil.which("rclone")
        using_rclone = use_rclone and (rclone_path is not None)

        if use_rclone and not rclone_path:
            logger.info("Rclone is not installed. Falling back to native Python replication.")

        _url = cls.url + "/replicate"

        try:
            # Fetch replication credentials
            response = requests.post(_url, headers=headers, json=payload)
            if response.status_code != 200:
                try:
                    err_msg = response.json().get("error", "Unknown error")
                except ValueError:
                    err_msg = response.text
                err_msg = f"Unifier replicate error: {err_msg}"
                logger.warning(err_msg)
                print(err_msg)
                return

            resp_json = response.json()

            data = resp_json.get("data")
            if not data:
                err_msg = "Unifier replicate response missing 'data' field"
                logger.warning(err_msg)
                print(err_msg)
                return

            access_key_id = data.get("access_key_id")
            secret_access_key = data.get("secret_access_key")
            data_path = resp_json.get("data_path")
            folders = resp_json.get("folders", [])
            endpoint = resp_json.get("endpoint", "https://s3.wasabisys.com")
            region = resp_json.get("region", "us-east-1")

            if not (access_key_id and secret_access_key and data_path):
                err_msg = "Unifier replicate missing required credentials or path"
                logger.warning(err_msg)
                print(err_msg)
                return

            # Handle s3:// or s3a:// prefix
            if data_path.startswith("s3://"):
                data_path = data_path[5:]
            elif data_path.startswith("s3a://"):
                data_path = data_path[6:]

            if using_rclone:
                # Prepare rclone environment
                env = os.environ.copy()
                env["RCLONE_CONFIG_UNIFIER_TYPE"] = "s3"
                env["RCLONE_CONFIG_UNIFIER_PROVIDER"] = "Wasabi"
                env["RCLONE_CONFIG_UNIFIER_ENV_AUTH"] = "false"
                env["RCLONE_CONFIG_UNIFIER_ACCESS_KEY_ID"] = access_key_id
                env["RCLONE_CONFIG_UNIFIER_SECRET_ACCESS_KEY"] = secret_access_key
                env["RCLONE_CONFIG_UNIFIER_ENDPOINT"] = endpoint
                env["RCLONE_CONFIG_UNIFIER_REGION"] = region

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
                    if folder.endswith("*"):
                        folder = folder[:-1] + "**"
                    cmd.extend(["--include", folder])

                print(f"Starting rclone replication for {name}...")
                subprocess.run(cmd, env=env, check=True)
                print(f"Replication completed for {name}")
            else:
                cls._replicate_native(
                    access_key_id=access_key_id,
                    secret_access_key=secret_access_key,
                    endpoint=endpoint,
                    region=region,
                    data_path=data_path,
                    target_location=target_location,
                    folders=folders,
                    name=name
                )

        except requests.exceptions.RequestException as e:
            err_msg = f"Replicate network request failed: {e}"
            logger.error(err_msg)
            print(err_msg)
        except subprocess.CalledProcessError as e:
            err_msg = f"Rclone execution failed: {e}"
            logger.error(err_msg)
            print(err_msg)
        except Exception as e:
            err_msg = f"An unexpected error occurred during replication: {e}"
            logger.error(err_msg)
            print(err_msg)

    @classmethod
    def _replicate_native(
        cls,
        access_key_id: str,
        secret_access_key: str,
        endpoint: str,
        region: str,
        data_path: str,
        target_location: str,
        folders: List[str],
        name: str
    ) -> None:
        """Fallback replication using boto3."""
        if boto3 is None:
            logger.error("boto3 is not installed. Cannot use native replication fallback.")
            logger.info("Please install boto3: pip install boto3")
            return

        session = boto3.Session()
        s3 = session.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint,
            region_name=region,
            config=Config(signature_version='s3v4')
        )
        
        parts = data_path.split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        match_patterns = []
        for f in folders:
            if f.startswith("/"):
                f = f.lstrip("/")
            match_patterns.append(f)

        print(f"Starting native python replication for {name} (parallel)...")

        # Configure transfer settings for parallel parts download within a single file
        transfer_config = TransferConfig(
            multipart_threshold=1024 * 25,  # 25MB
            max_concurrency=10
        )

        try:
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            download_tasks = []
            for page in pages:
                if 'Contents' not in page:
                    continue
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # Calculate relative path
                    if not key.startswith(prefix):
                         continue
                    rel_path = key[len(prefix):]
                    if not rel_path:
                        continue 

                    # Filter
                    if match_patterns:
                        if not any(fnmatch.fnmatch(rel_path, p) for p in match_patterns):
                            continue
                    
                    local_file = os.path.join(target_location, rel_path)
                    download_tasks.append((bucket_name, key, local_file))
            
            total_files = len(download_tasks)
            if total_files == 0:
                print(f"No files found to replicate for {name}")
                return

            print(f"Found {total_files} files to download.")

            def _download_one(args):
                b_name, k, l_file = args
                local_dir = os.path.dirname(l_file)
                os.makedirs(local_dir, exist_ok=True)
                # s3 client is thread-safe
                s3.download_file(b_name, k, l_file, Config=transfer_config)

            # Use ThreadPoolExecutor for parallel file downloads
            completed_count = 0
            with ThreadPoolExecutor(max_workers=10) as executor:
                # We use list() to consume the iterator and wait for all tasks to complete
                for _ in executor.map(_download_one, download_tasks):
                    completed_count += 1
                    if completed_count % 10 == 0:
                        print(f"Downloaded {completed_count}/{total_files} files...")
            
            print(f"Native replication completed for {name}. Downloaded {total_files} files.")
        except Exception as e:
            err_msg = f"Native replication failed: {e}"
            logger.error(err_msg)
            print(err_msg)
