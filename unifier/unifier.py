import datetime
import itertools
import json
import logging
import os
import re
import subprocess
import shutil
import urllib3.exceptions
from ijson.backends import yajl2_c
from typing import Any, Dict, Iterator, List, Optional, Sequence
from urllib.parse import urljoin
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

# Sentinel default for client_version: send this client's actual version.
# A caller-supplied string is sent as-is; explicit None opts out entirely
# (no client_version field is sent).
_AUTO_CLIENT_VERSION = object()


class _BodyError(Exception):
    # Internal: the query body transport failed. The failure has already
    # been logged at its source (the long-poll path has also sent its
    # cleanup DELETE); this carries the failure to the consuming front,
    # which decides what returning it looks like.
    pass


class _BodyStream:
    # Iterable of raw query-response body bytes, yielding them as they
    # arrive (urllib3 read1; a hard error if read1 is unavailable, no
    # fallback). The 200/202 decision happens inside the transport, based
    # on what the server answers: a direct 200 streams its own body; a 202
    # streams the long-polled Location resource with the backoff/salvage
    # machinery. `location` records where the body came from (None for a
    # direct 200) so a front can clean up after interpretation failures
    # without knowing which path the transport took. A future front can
    # consume these bytes with a different interpretation than query().
    # A consumer that abandons a long-polled stream mid-way sends the
    # cleanup DELETE to its location (GeneratorExit), just like a real
    # transport failure does.

    def __init__(self, client, payload):
        self.client = client
        self.payload = payload
        self.location = None

    def abandon(self):
        # A front ended this body without completing it (abandonment,
        # interpretation failure, or a spec violation): clean up the
        # long-polled location explicitly — nested-generator cleanup via
        # GC is unreliable (ijson's Cython objects are cycle-collected) —
        # and clear it so the delayed inner cleanup cannot delete twice.
        # Not for transport real errors: the transport already sent its
        # cleanup DELETE before raising.
        if self.location is not None:
            self.client._cleanup_delete(
                self.location,
                "Unifier query long-poll abandoned before completion",
            )
            self.location = None

    def __iter__(self):
        pool = urllib3.PoolManager()
        headers = {"Content-Type": "application/json"}
        try:
            response = pool.request(
                "POST",
                self.client.url,
                body=json.dumps(self.payload).encode(),
                headers=headers,
                retries=0,
                preload_content=False,
            )
        except urllib3.exceptions.HTTPError as e:
            logger.error("Unifier query request failed: %s", e)
            raise _BodyError() from e

        if response.status == 202:
            # Accepted: the response is forming at the Location resource
            # (a generic id, not a file). Location is a relative reference:
            # resolve it against the request URL that produced the 202,
            # never against a configured base url. retries=0 means no
            # redirect following, so the POST URL is exactly that request
            # URL (urllib3's response.url is not the full URL here).
            location = response.headers.get("Location")
            response.close()
            if not location:
                logger.warning(
                    "Unifier query 202 response missing Location header"
                )
                raise _BodyError()
            self.location = urljoin(self.client.url, location)
            try:
                yield from self.client._range_poll(pool, self.location)
            except GeneratorExit:
                # The consumer abandoned the stream mid-long-poll: the
                # location is an incomplete resource, so clean it up like
                # any other incomplete query. (GeneratorExit must be
                # re-raised after cleanup.) Skipped when a front has
                # already abandoned explicitly (location cleared).
                if self.location is not None:
                    self.client._cleanup_delete(
                        self.location,
                        "Unifier query long-poll abandoned before completion",
                    )
                raise
            return

        if response.status != 200:
            # A transport cut of the error body is transport interference,
            # not a server bug (which would fail loudly): treat it like an
            # unparseable error body and fall to the status-code log.
            try:
                body = response.read()
            except urllib3.exceptions.HTTPError:
                body = b""
            response.close()
            try:
                err = json.loads(body)
            except ValueError:
                err = {}
            if isinstance(err, dict) and "error" in err:
                logger.warning("Unifier query error: %s", err["error"])
                raise _BodyError()

            logger.warning(
                "Unifier query failed with status code %s", response.status
            )
            raise _BodyError()

        # 200: stream the response body as it arrives. A cut on the direct
        # body has no cursor to advance (that is the long-poll path's
        # salvage), so it fails the query like any other transport error.
        try:
            while True:
                piece = response.read1(65536)
                if not piece:
                    break
                yield piece
        except urllib3.exceptions.HTTPError as e:
            logger.error("Unifier query request failed: %s", e)
            raise _BodyError() from e
        finally:
            response.close()


class _BodyFile:
    # File-like adapter over a byte-piece iterable (a _BodyStream) for the
    # ijson parsers, which pull via read(amt). Each read serves from the
    # buffer and, when the buffer is empty, pulls at most one new piece
    # (short reads are legal and ijson tolerates them, so pieces are never
    # aggregated and pacing stays the transport's). `pulled` snapshots
    # every byte taken from the piece source, so a new adapter can replay
    # what an abandoned probe pass (the stream root check) consumed.

    def __init__(self, pieces):
        self._pieces = iter(pieces)
        self._buf = b""
        self._eof = False
        self.pulled = b""

    def read(self, amt=-1):
        if not self._buf and not self._eof:
            try:
                piece = next(self._pieces)
            except StopIteration:
                self._eof = True
            else:
                self._buf = piece
                self.pulled += piece
        if amt == -1:
            data, self._buf = self._buf, b""
        else:
            data = self._buf[:amt]
            self._buf = self._buf[amt:]
        return data

    def detach(self):
        # Drop the piece-source reference: an abandoned probe pass (its
        # ijson object has no close and may be cycle-collected) must not
        # pin the byte generator and delay the abandonment cleanup DELETE.
        self._pieces = iter(())


def _normalize_date(value: Any, field: str) -> Optional[str]:
    """Coerce a date input into the `YYYY-MM-DD` string the API expects.

    Accepts `None` (passed through), a `datetime.date`/`datetime.datetime`, or a
    string already in `YYYY-MM-DD` form.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return datetime.datetime.strptime(value.strip(), "%Y-%m-%d").date().isoformat()
        except ValueError:
            raise ValueError(
                f"{field} must be in YYYY-MM-DD format, got {value!r}"
            ) from None
    raise TypeError(
        f"{field} must be a date or a YYYY-MM-DD string, got {type(value).__name__}"
    )


def _apply_catalog_scope(
    payload: Dict[str, Any],
    show_all_datasets: bool,
    product_type: Optional[str],
) -> None:
    """Add the public-display options to a data catalog request payload.

    Fields left unset are omitted from the payload entirely.
    """
    if product_type and not show_all_datasets:
        logger.warning(
            "product_type=%r has no effect unless show_all_datasets is True",
            product_type,
        )
    if show_all_datasets:
        payload["show_all_datasets"] = True
    if product_type:
        payload["product_type"] = product_type


class Unifier:
    """Client helpers to interact with the Unifier API.

    Notes
    -----
    - Credentials can be set via the class variables `user` and `token` or
      passed per-call to the query helpers.
    - The `as_of` parameter is deprecated; prefer `asof_date` and
      `asof_back_to`.
    """
    
    url = 'https://unifier.x-tech.ai/unifier'
    user = ''
    token = ''

    # Long-poll Prefer: wait backoff (seconds), longest first. An error on
    # the post-202 GET path steps down one level; the level is sticky for
    # the process (not persisted) and climbs back one level every `rampup`
    # error-free queries. `wait`, when set, caps the sequence: it becomes
    # the longest value and only lower stock values apply.
    BACKOFF_WAITS = (86400, 3600, 900, 300, 30, 15, 5, 1)
    wait = None
    rampup = 10
    _wait_level = 0
    _clean_queries = 0

    __version__ = '0.1.24'

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
    def list_data_catalog(
        cls,
        show_all_datasets: bool = False,
        product_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve the datasets the user is entitled to see.

        Parameters
        ----------
        show_all_datasets : bool
            When True, publicly available datasets are returned in addition to
            the ones you are entitled to. Defaults to False.
        product_type : Optional[str]
            Limit the publicly available datasets to a single product. Your
            entitled datasets are always returned regardless of product, so
            this has no effect unless `show_all_datasets` is True.

        Returns
        -------
        list[dict]
            A list of dataset metadata objects. Returns an empty list on error.
        """
        headers = {"Content-Type": "application/json"}
        payload = {
            "user": cls.user,
            "token": cls.token,
        }
        _apply_catalog_scope(payload, show_all_datasets, product_type)

        _url = cls.url + "/list-data-catalogs"
        try:
            response = requests.post(_url, headers=headers, json=payload)
            if response.status_code != 200:
                try:
                    err = response.json()
                except ValueError:
                    err = {}
                if isinstance(err, dict) and "error" in err:
                    logger.warning("Unifier list-data-catalog error: %s", err["error"])
                else:
                    logger.warning("Unifier list-data-catalog failed with status code %s", response.status_code)
                return []

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Unifier list-data-catalog request failed: %s", e)
            return []

    @classmethod
    def list_entitled_datasets(
        cls,
        date: Optional[Any] = None,
        from_date: Optional[Any] = None,
        to_date: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve the datasets the user is entitled to.

        With no arguments this returns the datasets entitled right now. Pass
        `date` for a single day, or `from_date`/`to_date` for a range. A dataset
        is included when its entitlement window overlaps the window asked about.

        Parameters
        ----------
        date : Optional[str | datetime.date]
            A single day to check. Cannot be combined with `from_date` or
            `to_date`.
        from_date : Optional[str | datetime.date]
            Start of an inclusive range. Given on its own, matches entitlements
            active on or after this date.
        to_date : Optional[str | datetime.date]
            End of an inclusive range. Given on its own, matches entitlements
            active on or before this date.

        Returns
        -------
        list[dict]
            Dataset metadata objects sorted by `unifier_source_name`. Returns an
            empty list on error.

        Raises
        ------
        ValueError
            If `date` is combined with a range, a date is not `YYYY-MM-DD`, or
            `from_date` is later than `to_date`.

        Notes
        -----
        Only datasets that are currently live are returned, including for
        historical dates. A dataset retired since then will not appear even if
        the entitlement was active at the time.
        """
        if date is not None and (from_date is not None or to_date is not None):
            raise ValueError(
                "'date' cannot be combined with 'from_date' or 'to_date'"
            )

        _date = _normalize_date(date, "date")
        _from_date = _normalize_date(from_date, "from_date")
        _to_date = _normalize_date(to_date, "to_date")

        if _from_date and _to_date and _from_date > _to_date:
            raise ValueError(
                f"from_date ({_from_date}) cannot be later than to_date ({_to_date})"
            )

        headers = {"Content-Type": "application/json"}
        payload = {
            "user": cls.user,
            "token": cls.token,
        }
        # Omit unset date fields entirely so the server applies its own default.
        if _date:
            payload["date"] = _date
        if _from_date:
            payload["from_date"] = _from_date
        if _to_date:
            payload["to_date"] = _to_date

        _url = cls.url + "/list-entitled-datasets"
        try:
            response = requests.post(_url, headers=headers, json=payload)
            if response.status_code != 200:
                try:
                    err = response.json()
                except ValueError:
                    err = {}
                detail = None
                if isinstance(err, dict):
                    detail = err.get("error") or err.get("detail")
                if detail:
                    logger.warning("Unifier list-entitled-datasets error: %s", detail)
                else:
                    logger.warning(
                        "Unifier list-entitled-datasets failed with status code %s",
                        response.status_code,
                    )
                return []

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Unifier list-entitled-datasets request failed: %s", e)
            return []

    @classmethod
    def get_dataset_details(
        cls,
        name: str,
        show_all_datasets: bool = False,
        product_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Retrieve detailed metadata for a specific dataset.

        Parameters
        ----------
        name : str
            The dataset name.
        show_all_datasets : bool
            When True, the dataset is also returned if it is publicly
            available, even without an entitlement for it. Defaults to False.
        product_type : Optional[str]
            Limit the publicly available datasets to a single product. Has no
            effect unless `show_all_datasets` is True.

        Returns
        -------
        dict
            The dataset metadata object. Returns an empty dict on error.
        """
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": name,
            "user": cls.user,
            "token": cls.token,
        }
        _apply_catalog_scope(payload, show_all_datasets, product_type)

        _url = cls.url + "/data-catalog"
        try:
            response = requests.post(_url, headers=headers, json=payload)
            if response.status_code != 200:
                try:
                    err = response.json()
                except ValueError:
                    err = {}
                if isinstance(err, dict) and "error" in err:
                    logger.warning("Unifier get-dataset-details error: %s", err["error"])
                else:
                    logger.warning("Unifier get-dataset-details failed with status code %s", response.status_code)
                return {}

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Unifier get-dataset-details request failed: %s", e)
            return {}

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
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
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
        client_version : Optional[str]
            Client version string sent with the request. Defaults to this
            client's actual version; a caller-supplied string is sent
            as-is; explicit None sends no client_version field.

        Returns
        -------
        list
            The JSON payload returned by the API (commonly a list of lists of
            dicts). Returns an empty list on error.
        """

        payload = cls._query_payload(
            name, user, token, key, keys, as_of, back_to, up_to,
            asof_date, asof_back_to, limit, column_filters, disable_view,
            client_version,
        )

        stream = cls._body_stream(payload)
        try:
            body = b"".join(stream)
        except _BodyError:
            # The transport already logged the failure (and sent its
            # cleanup DELETE for the long-poll path).
            return []
        try:
            response_data = json.loads(body)
        except ValueError as e:
            # Delivered bytes are not JSON: same outcome and log as ever,
            # plus post-202 cleanup when the body came from a Location.
            message = f"Unifier query request failed: {e}"
            if stream.location is not None:
                cls._cleanup_delete(stream.location, message)
            else:
                logger.error(message)
            return []
        if not isinstance(response_data, list):
            message = f"Unexpected query response format: {type(response_data)!r}"
            if stream.location is not None:
                cls._cleanup_delete(stream.location, message)
            else:
                logger.warning(message)
            return []
        return response_data

    @classmethod
    def _query_payload(
        cls,
        name: str,
        user: Optional[str],
        token: Optional[str],
        key: Optional[str],
        keys: Optional[Sequence[str]],
        as_of: Optional[str],
        back_to: Optional[str],
        up_to: Optional[str],
        asof_date: Optional[str],
        asof_back_to: Optional[str],
        limit: Optional[int],
        column_filters: Optional[str],
        disable_view: bool,
        client_version: Optional[str],
    ) -> Dict[str, Any]:
        # The query wire payload, shared by query() and stream().
        payload: Dict[str, Any] = {
            "name": name,
            "user": cls.user,
            "token": cls.token,
            "disable_view": disable_view,
        }

        if client_version is not None:
            if client_version is _AUTO_CLIENT_VERSION:
                client_version = cls.__version__
            payload["client_version"] = client_version

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

        return payload

    @classmethod
    def _body_stream(cls, payload: Dict[str, Any]) -> _BodyStream:
        # The body-byte transport for query() and stream(): one stream that
        # owns the 200/202 dispatch (see _BodyStream).
        return _BodyStream(cls, payload)

    @classmethod
    def _wait_sequence(cls) -> Sequence[int]:
        if cls.wait is None:
            return cls.BACKOFF_WAITS
        return (cls.wait,) + tuple(v for v in cls.BACKOFF_WAITS if v < cls.wait)

    @classmethod
    def _cleanup_delete(cls, location: str, message: str) -> None:
        # Real error post-202: best-effort DELETE cleanup. We do not care
        # whether it succeeds; a failed DELETE is appended info on the
        # error we already have, never a new error. The server's own
        # cleanup does not strictly depend on this DELETE arriving.
        cleanup = ""
        try:
            response = requests.delete(location)
            if not 200 <= response.status_code < 300:
                cleanup = (
                    f"; cleanup DELETE failed with status code {response.status_code}"
                )
        except requests.exceptions.RequestException as e:
            cleanup = f"; cleanup DELETE failed: {e}"
        logger.warning("%s%s", message, cleanup)

    @classmethod
    def _range_poll(cls, pool, location: str) -> Iterator[bytes]:
        # Long-poll a 202-formed response: open-ended Range GETs from the
        # next unread byte, with Prefer: wait=<seconds> so the server can
        # hold each request until more bytes exist. The GETs use urllib3
        # directly (no retries: this loop owns retrying) and yield the 206
        # body as it streams: a transport failure (not an HTTP error) that
        # cuts the stream mid-body keeps the bytes already read as data,
        # and the retry advances the range cursor past them instead of
        # re-requesting the same range — a response that was progressing
        # toward 206 but cut by a proxy or MITM still contributes. 204
        # means nothing new yet (re-request the same cursor). Any other
        # GET error steps down the sticky backoff sequence one level; an
        # error while already at the shortest wait is a real error and
        # sends a best-effort DELETE to the location before raising. The
        # server conforms to the protocol spec, so Content-Range is
        # trusted and never validated: a non-conforming value is a hard
        # error (the parse fails loudly), not handled — consistent with
        # the no-fallback read1 policy. Intermediate 206s carry
        # Content-Range "bytes x-y/*" (total unknown); the response is
        # complete once a numeric total is reported and fully received.
        # Yields the assembled body's bytes; the consuming front joins
        # and interprets them through the same path as a 200 response.
        seq = cls._wait_sequence()
        level = cls._wait_level
        offset = 0
        while True:
            headers = {"Range": f"bytes={offset}-", "Prefer": f"wait={seq[level]}"}
            http_detail = None
            transport_detail = None
            response = None
            salvaged = 0
            total = None
            try:
                response = pool.request(
                    "GET",
                    location,
                    headers=headers,
                    retries=0,
                    preload_content=False,
                )
            except urllib3.exceptions.HTTPError as e:
                transport_detail = str(e)

            if response is not None and response.status == 204:
                # Nothing new yet: ignore and re-request the same range
                # cursor. The loop builds each Range from `offset`, which
                # only advances when bytes arrive, so `continue` re-asks
                # for the same open-ended byte range.
                response.close()
                continue

            if response is not None and response.status != 206:
                # HTTP-level error: no body is collected from it.
                http_detail = f"status code {response.status}"
                response.close()
            elif response is not None:
                # 206. Trust the Content-Range the server reports.
                match = re.fullmatch(
                    r"bytes (\d+)-(\d+)/(\d+|\*)",
                    response.headers.get("Content-Range", "").strip(),
                )
                end = int(match.group(2))
                complete = match.group(3)
                total = int(complete) if complete != "*" else None
                # Stream the body: read1 returns bytes as they arrive
                # instead of blocking for the full declared length, so a
                # transport cut mid-body keeps everything that arrived —
                # read1 has already returned it by the time it raises.
                # (urllib3 without read1 is a hard error: no fallback.)
                try:
                    while True:
                        piece = response.read1(65536)
                        if not piece:
                            break
                        yield piece
                        salvaged += len(piece)
                except urllib3.exceptions.HTTPError as e:
                    transport_detail = str(e)
                finally:
                    response.close()
                if transport_detail is None:
                    # The whole declared chunk arrived.
                    offset = end + 1
                    if total is not None and offset >= total:
                        break
                    continue
                # Transport cut mid-body: the bytes read are data; advance
                # the cursor past them for the retry. If they already reach
                # the reported total, the cut lost nothing: complete.
                offset += salvaged
                if total is not None and offset >= total:
                    break

            # An error (HTTP-level or transport): back off one wait level
            # and retry, or fail for real at the minimum wait.
            if level + 1 < len(seq):
                level += 1
                cls._clean_queries = 0
                continue
            cls._wait_level = level
            cls._cleanup_delete(
                location,
                f"Unifier query long-poll failed with"
                f" {transport_detail or http_detail}"
                f" at minimum wait {seq[level]}",
            )
            raise _BodyError()
        # Success: the level is sticky; every `rampup` error-free queries
        # at a backed-off level climbs one level back up.
        cls._wait_level = level
        if level > 0:
            cls._clean_queries += 1
            if cls._clean_queries >= cls.rampup:
                cls._wait_level = level - 1
                cls._clean_queries = 0

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
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
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
            client_version=client_version,
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
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
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
            client_version=client_version,
        )
        if not json_result:
            return []
        extracted_data = [
            {k: v for d in item for k, v in d.items()} for item in json_result
        ]
        return extracted_data

    @classmethod
    def stream(
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
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Stream the query result row by row as it arrives.

        Same parameters and same transport as query() (including the 202
        long-poll path and its salvage/backoff machinery), but rows are
        yielded as soon as each one completes instead of waiting for the
        whole body: a front can watch the response roll in as the server
        publishes it. Parsing is incremental (ijson, yajl2_c C backend
        confirmed at import: no fallback) and numbers are floats, matching
        json.loads, so rows carry identical value types to query().

        Each yielded row is the row's list of fragment dicts, exactly as
        query() returns them; merge fragments as get_dataframe does if a
        flat record is wanted. There is no yield before the first complete
        row.

        Yields
        ------
        list[dict]
            One row (a list of fragment dicts) per yield.

        Raises
        ------
        ValueError
            The response root is not the query list (a conforming server
            always sends the list; items() alone would silently yield
            nothing for other roots).
        _BodyError
            The body transport failed (already logged; the long-poll path
            also sent its cleanup DELETE).
        IncompleteJSONError
            The delivered bytes ended before the JSON document did.

        Abandoning the generator mid-stream sends the cleanup DELETE to a
        long-polled location, just like any other incomplete query.
        """
        payload = cls._query_payload(
            name, user, token, key, keys, as_of, back_to, up_to,
            asof_date, asof_back_to, limit, column_filters, disable_view,
            client_version,
        )
        body = cls._body_stream(payload)
        pieces = iter(body)
        # Cleanup is explicit on every exit path: cleanup via nested
        # generator GC is unreliable (ijson's Cython objects are
        # cycle-collected), so stream() owns the DELETE and the transport
        # generator's unwinding itself.
        try:
            # Root contract: items() over a non-array root yields nothing
            # and no error, so the root is verified from the first parse
            # event. The probe adapter snapshots what the probe consumed;
            # the items pass replays the snapshot, then continues the same
            # byte generator. The probe adapter is detached after: the
            # probe's ijson object has no close() and may be
            # cycle-collected, so it must not pin the byte generator.
            probe = _BodyFile(pieces)
            first = next(yajl2_c.parse(probe), None)
            probe.detach()
            if first is None or first[0] != "" or first[1] != "start_array":
                raise ValueError(f"Unexpected stream response root: {first!r}")
            f = _BodyFile(itertools.chain([probe.pulled], pieces))
            for row in yajl2_c.items(f, "item", use_float=True):
                yield row
            # The document is fully parsed, but the transport generator is
            # still suspended at its final yield: drain it so its
            # completion bookkeeping (sticky wait level, rampup) runs.
            for _ in pieces:
                pass
            # Completed: the resource is finished, nothing to clean up.
            body.location = None
        except GeneratorExit:
            body.abandon()
            raise
        except _BodyError:
            raise  # transport real error: already logged and DELETEd
        except Exception:
            # Interpretation/spec failure (ValueError root,
            # IncompleteJSONError truncation): the location is an
            # incomplete or unusable resource — clean it up.
            body.abandon()
            raise
        finally:
            # Unwind the transport generator promptly no matter how the
            # stream ends: it closes its in-flight response; its own
            # cleanup sees a cleared location, so it cannot delete twice.
            pieces.close()

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
                cmd = [
                    "rclone", "copy", source, target_location,
                    "--progress", "--config", "/dev/null",
                    "--retries", "5", "--retries-sleep", "10s",
                ]

                if bandwidth_limit:
                     cmd.extend(["--bwlimit", f"{bandwidth_limit}M"])

                for folder in folders:
                    if folder.startswith("/"):
                        folder = folder.lstrip("/")
                    if folder.endswith("*"):
                        folder = folder[:-1] + "**"
                    cmd.extend(["--include", folder])

                print(f"Downloading with rclone for {name}...")
                result = subprocess.run(cmd, env=env)
                # rclone exit code 5 = temporary error (network blip); data was
                # still transferred. Warn rather than raise so callers aren't
                # misled into thinking nothing downloaded.
                if result.returncode == 5:
                    msg = (
                        f"Replication completed for {name} with minor network errors "
                        "(some files may need to be retried)."
                    )
                    logger.warning(msg)
                    print(msg)
                elif result.returncode != 0:
                    raise subprocess.CalledProcessError(result.returncode, cmd)
                else:
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
        """Replication using native Python (boto3)."""
        if boto3 is None:
            logger.error("boto3 is not installed. Cannot use native Python implementation.")
            logger.info("Please install boto3: pip install boto3")
            return

        session = boto3.Session()
        # botocore >= 1.36 enables request/response data-integrity checksums by
        # default. Wasabi (and other S3-compatible stores) reject these on
        # multipart/ranged GETs, returning SignatureDoesNotMatch. Disable them
        # when the running botocore supports the options.
        config_kwargs = {'signature_version': 's3v4'}
        try:
            s3_config = Config(
                request_checksum_calculation='when_required',
                response_checksum_validation='when_required',
                **config_kwargs,
            )
        except TypeError:
            s3_config = Config(**config_kwargs)

        s3 = session.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint,
            region_name=region,
            config=s3_config,
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

        print(f"Downloading using native python implementation for {name}...")

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
                # s3 client is thread-safe. Use boto3's default TransferConfig
                # (8 MB multipart threshold) so small files use a single GET.
                s3.download_file(b_name, k, l_file)

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
