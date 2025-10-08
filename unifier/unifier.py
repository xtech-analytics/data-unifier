import logging
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

    __version__ = '0.1.12'

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
            response = requests.post(_url, headers=headers, json=payload, timeout=30)
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
            response = requests.post(cls.url, headers=headers, json=payload, timeout=60)
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
