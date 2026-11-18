"""Client helpers to interact with the Unifier API.

The client knows no query parameters: everything beyond the dataset name
and credentials passes through to the server verbatim, so the parameters
cannot be documented here. The protocol is the contract, documented by the
server's OpenAPI document — call `Unifier.help()` to fetch and print it
over the internet.
"""

import datetime
import email.utils
import itertools
import json
import logging
import os
import re
import reprlib
import subprocess
import shutil
import sys
import time
import urllib3.exceptions
import warnings
from collections.abc import Iterable
from ijson.backends import yajl2_c
from typing import Any, Dict, Iterator, List, Optional, Sequence
from urllib.parse import urljoin, urlparse
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

# Sentinel default for client_version: send this client's actual version,
# or UNIFIER_CLIENT_VERSION_OVERRIDE when set at import. A caller-supplied
# string is sent as-is; explicit None opts out entirely (no client_version
# field is sent, legacy mode).
_AUTO_CLIENT_VERSION = object()

# Positional arguments are deprecated: every parameter is keyword-only. For
# compatibility, each method still accepts the positional orders it accepted
# before 0.1.24 — and only those (frozen here; nothing new is ever added) —
# and warns with the equivalent named call.
_LEGACY_QUERY_ORDER = (
    "name", "user", "token", "key", "keys", "as_of", "back_to", "up_to",
    "asof_date", "asof_back_to", "limit", "column_filters", "disable_view",
)
_LEGACY_REPLICATE_ORDER = (
    "name", "target_location", "asof_date", "back_to", "up_to",
    "bandwidth_limit", "use_rclone",
)
_SECRET_ARGS = ("user", "token")   # never echoed in a warning
_DEFAULT_TARGET = "."   # replicate(): download into the current directory
_warn_repr = reprlib.Repr()
_warn_repr.maxstring = 80
_warn_repr.maxother = 80


def _warn_deprecated(message: str, stacklevel: int) -> None:
    # Attribute the deprecation to the caller's line: walk that many
    # frames up from the calling function (stacklevel=1 is the calling
    # function itself, as warnings.warn counts), treating eval'd
    # "<string>" frames as transparent — they carry no source text, so
    # a warning landing there would print without its source line; the
    # warning belongs on the line that textually contains the call.
    frame = sys._getframe(1)
    for _ in range(stacklevel - 1):
        frame = frame.f_back
        while frame is not None and frame.f_code.co_filename.startswith("<string"):
            frame = frame.f_back
    if frame is None:
        frame = sys._getframe(1)
    warnings.warn_explicit(
        message, FutureWarning,
        frame.f_code.co_filename, frame.f_lineno,
        module=frame.f_globals.get("__name__", "<unknown>"),
        registry=frame.f_globals.setdefault("__warningregistry__", {}),
    )


def _legacy_positional(
    method: str,
    order: Sequence[str],
    args: Sequence[Any],
    named: Dict[str, Any],
    defaults: Optional[Dict[str, Any]] = None,
    stacklevel: int = 3,
) -> Dict[str, Any]:
    """Bind deprecated positional `args` to their names in `order`.

    Returns `named` updated with the positional values (a None positional
    means "not given", as it always did). Raises TypeError as Python would
    for too many positionals or one also given by name. Warns (FutureWarning,
    shown once per calling line) with the equivalent named call; credentials
    are masked.
    """
    defaults = defaults or {}
    if len(args) > len(order):
        raise TypeError(
            f"{method}() takes at most {len(order)} positional arguments "
            f"({len(args)} given)"
        )
    out = dict(named)
    for key, value in zip(order, args):
        if key in named and named[key] is not defaults.get(key):
            raise TypeError(f"{method}() got multiple values for argument {key!r}")
        if value is not None:
            out[key] = value
    # The whole equivalent call: positional values plus what was named.
    shown = [
        f"{key}=..." if key in _SECRET_ARGS else f"{key}={_warn_repr.repr(value)}"
        for key, value in out.items()
        if value is not None and value is not defaults.get(key)
    ]
    _warn_deprecated(
        f"unifier.{method}(): positional arguments are deprecated and support "
        f"for them will be removed in a future release; use named arguments: "
        f"{method}({', '.join(shown)})",
        stacklevel,
    )
    return out


class UnifierError(Exception):
    """A Unifier request failed (transport, HTTP status, or the response
    body could not be interpreted as the expected JSON).

    Raised only when client_version is sent — the default, the new
    protocol: with parameters passing through verbatim, the server is the
    only parameter speller the request will ever meet, so swallowing its
    rejection into an empty result would make a typo indistinguishable
    from a legitimate empty response. client_version=None (legacy mode,
    also selectable process-wide via UNIFIER_CLIENT_VERSION_OVERRIDE=none)
    keeps the pre-0.1.24 contract: log and return the empty result.
    """


class _BodyError(Exception):
    # Internal: the query body transport failed. The failure has already
    # been logged at its source (the long-poll path has also sent its
    # cleanup DELETE); this carries the failure message to the consuming
    # front, which decides what returning it looks like.
    def __init__(self, message: str = ""):
        super().__init__(message)
        self.message = message


class _BodyStream:
    # Iterable of raw query-response body bytes, yielding them as they
    # arrive (r.raw.read1 — urllib3's read1 under requests; a hard error
    # if read1 is unavailable, no fallback). The transport rides
    # requests.Session so environment policy — proxies (HTTP(S)_PROXY,
    # NO_PROXY, OS settings), CA bundles (REQUESTS_CA_BUNDLE/
    # CURL_CA_BUNDLE/certifi), netrc, User-Agent — matches requests,
    # and therefore matches 0.1.23 and the catalog calls, by
    # construction. Redirects are followed (requests' default, 0.1.23's
    # behavior): the server may relocate the POST with 307/308 — 301/302
    # are invalid for POST relocation (requests demotes the method) and
    # fail loudly through the status handling by construction. The
    # 200/202 decision happens inside the transport, based
    # on what the server answers: a direct 200 streams its own body; a 202
    # streams the long-polled Location resource with the backoff/salvage
    # machinery. `location` records where the body came from (None for a
    # direct 200) so a front can clean up after interpretation failures
    # without knowing which path the transport took. A future front can
    # consume these bytes with a different interpretation than query().
    # A consumer that abandons a long-polled stream mid-way sends the
    # cleanup DELETE to its location (GeneratorExit), just like a real
    # transport failure does.

    def __init__(self, client, payload, legacy_mode=False):
        self.client = client
        self.payload = payload
        self.legacy_mode = legacy_mode
        self.location = None
        self.session = None
        self.conn_epoch = None

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
                self.session,
                self.location,
                "Unifier query long-poll abandoned before completion",
            )
            self.location = None

    def __iter__(self):
        # One session for the whole query lifecycle: the POST, the
        # long-poll GETs, and any cleanup DELETE all ride the same
        # connection where possible. Send errors surface as
        # requests.exceptions (which wrap urllib3's); body-read errors
        # from r.raw.read1 stay urllib3's.
        session = requests.Session()
        self.session = session
        # Identify the client on every request this session makes (the
        # POST, the long-poll GETs, and any cleanup DELETE).
        session.headers["User-Agent"] = (
            f"unifier-python/{self.client.__version__}"
        )
        session.headers.update(self.client.headers)
        # The POST opens the connection; its age budget starts here.
        self.conn_epoch = self.client._now()
        headers = {"Content-Type": "application/json"}
        try:
            response = session.post(
                self.client.url,
                headers=headers,
                stream=True,
                **_post_body(self.payload, self.legacy_mode),
            )
        except requests.exceptions.RequestException as e:
            logger.error("Unifier query request failed: %s", e)
            raise _BodyError(f"Unifier query request failed: {e}") from e

        if response.status_code == 202:
            # Accepted: the response is forming at the Location resource
            # (a generic id, not a file). Location is a relative reference:
            # resolve it against the URL that actually answered the 202 —
            # response.url, the redirect target when the POST was
            # 307/308-relocated, the request URL otherwise — never against
            # a configured base url. 202 carries an empty body: drain it
            # and return the clean connection to the pool for the
            # long-poll GETs to reuse.
            location = response.headers.get("Location")
            response.content
            if not location:
                logger.warning(
                    "Unifier query 202 response missing Location header"
                )
                raise _BodyError(
                    "Unifier query 202 response missing Location header"
                )
            self.location = urljoin(response.url, location)
            try:
                yield from self.client._range_poll(self, self.location)
            except GeneratorExit:
                # The consumer abandoned the stream mid-long-poll: the
                # location is an incomplete resource, so clean it up like
                # any other incomplete query. (GeneratorExit must be
                # re-raised after cleanup.) Skipped when a front has
                # already abandoned explicitly (location cleared).
                if self.location is not None:
                    self.client._cleanup_delete(
                        session,
                        self.location,
                        "Unifier query long-poll abandoned before completion",
                    )
                raise
            return

        if response.status_code != 200:
            # A transport cut of the error body is transport interference,
            # not a server bug (which would fail loudly): treat it like an
            # unparseable error body and fall to the status-code log.
            try:
                body = response.content
            except (requests.exceptions.RequestException,
                    urllib3.exceptions.HTTPError):
                body = b""
            response.close()
            try:
                err = json.loads(body)
            except ValueError:
                err = {}
            if isinstance(err, dict) and "error" in err:
                logger.warning("Unifier query error: %s", err["error"])
                raise _BodyError(f"Unifier query error: {err['error']}")

            logger.warning(
                "Unifier query failed with status code %s",
                response.status_code,
            )
            raise _BodyError(
                f"Unifier query failed with status code {response.status_code}"
            )

        if "conflation" in self.payload:
            # A conflation stream request must be served by the long-poll
            # path (the response forms at a Location): a direct 200
            # violates the stream contract — a hard error, not a result.
            response.close()
            logger.warning(
                "Unifier stream conflation answered 200 (expected 202)"
            )
            raise _BodyError(
                "Unifier stream conflation answered 200 (expected 202)"
            )

        # 200: stream the response body as it arrives. decode_content is
        # explicit (True): this path advertises requests' default
        # Accept-Encoding, so the server may answer compressed and the
        # bytes must be decoded before interpretation. A cut on the direct
        # body has no cursor to advance (that is the long-poll path's
        # salvage), so it fails the query like any other transport error.
        try:
            while True:
                piece = response.raw.read1(65536, decode_content=True)
                if not piece:
                    break
                yield piece
        except urllib3.exceptions.HTTPError as e:
            logger.error("Unifier query request failed: %s", e)
            raise _BodyError(f"Unifier query request failed: {e}") from e
        finally:
            response.close()


class _BodyFile:
    # File-like adapter over a byte-piece iterable (a _BodyStream) for the
    # ijson parsers, which pull via read(amt). Each read serves from the
    # buffer and, when the buffer is empty, pulls at most one new piece
    # (short reads are legal and ijson tolerates them, so pieces are never
    # aggregated and pacing stays the transport's). `pulled` (opt-in via
    # snapshot=True) records every byte taken from the piece source, so a
    # new adapter can replay what an abandoned probe pass (the stream root
    # check) consumed; accumulating it over a whole body would copy O(n^2)
    # bytes, so the items pass never snapshots.

    def __init__(self, pieces, snapshot=False):
        self._pieces = iter(pieces)
        self._buf = b""
        self._eof = False
        self.snapshot = snapshot
        self.pulled = b""

    def read(self, amt=-1):
        if not self._buf and not self._eof:
            try:
                piece = next(self._pieces)
            except StopIteration:
                self._eof = True
            else:
                self._buf = piece
                if self.snapshot:
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


def _json_default(value: Any) -> Any:
    """Generic parameter encoding: what the JSON encoder cannot handle
    becomes what it means on the wire, by type — never by parameter name.

    The datetime family rides as ISO 8601 with full fidelity: a naive
    value stays naive (the server interprets or rejects), an aware value
    keeps its UTC offset (never collapsed to a wall-clock date), a date
    is a plain date. Any non-str/bytes iterable (set, generator, pandas
    index) becomes an array. bytes deliberately fails loudly rather than
    becoming a list of ints.
    """
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (set, frozenset)) or (
        isinstance(value, Iterable)
        and not isinstance(value, (str, bytes, bytearray, dict))
    ):
        return list(value)
    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable"
    )


def _retry_after(value: Optional[str], default: float) -> float:
    # Retry-After (RFC 9110): delay-seconds or an HTTP-date; anything
    # unparseable, or no header, means `default`. A date in the past is 0.
    if value is None:
        return default
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return default
    if when.tzinfo is None:
        when = when.replace(tzinfo=datetime.timezone.utc)
    return max(0.0, (when - datetime.datetime.now(datetime.timezone.utc)).total_seconds())


def _post_body(payload: Dict[str, Any], legacy_mode: bool) -> Dict[str, Any]:
    # The POST body keyword for requests. Legacy mode (client_version
    # None) encodes exactly as 0.1.23 did, through requests' json=: no
    # type conversion (a date or set is a TypeError before any request)
    # and no NaN/inf (requests raises InvalidJSONError, a
    # RequestException, which each caller's transport handling turns
    # into 0.1.23's soft-fail). Otherwise the generic JSON delegate.
    if legacy_mode:
        return {"json": payload}
    return {"data": json.dumps(payload, default=_json_default).encode()}


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
    warn: bool = True,
) -> None:
    """Add the public-display options to a data catalog request payload.

    Fields left unset are omitted from the payload entirely. The
    product_type-needs-show_all warning is compatibility-era advice:
    with client_version sent, whether product_type implies anything is
    the server's to say, so callers pass warn=False.
    """
    if warn and product_type and not show_all_datasets:
        logger.warning(
            "product_type=%r has no effect unless show_all_datasets is True",
            product_type,
        )
    if show_all_datasets:
        payload["show_all_datasets"] = True
    if product_type:
        payload["product_type"] = product_type


# --- help(): render the server's self-description --------------------------
#
# The document may be Swagger 2.0 (swaggo v1, what the server serves
# today) or OpenAPI 3.0/3.1/3.2 (Go generators, swaggo v2, huma), all as
# JSON. The version branches only to find each operation's body schema —
# `in: body` in 2.0, `requestBody` in 3.x — and every openapi 3.x is
# handled with the 3.1 rules (3.2 only adds; a future 3.3 most likely
# only adds). Everything else is common, tolerant code.

_PATH_ITEM_NON_OPS = {
    "parameters", "summary", "description", "servers", "variables", "$ref",
}


def _doc_flavor(doc: Any) -> Optional[str]:
    # "swagger2" | "openapi3" | None (unknown: the caller prints the raw
    # document rather than guessing).
    if not isinstance(doc, dict):
        return None
    v = doc.get("swagger")
    if isinstance(v, str) and v.startswith("2"):
        return "swagger2"
    v = doc.get("openapi")
    if isinstance(v, str) and v.startswith("3"):
        return "openapi3"
    return None


def _base_prefix(doc: Dict[str, Any], flavor: str) -> str:
    # The path prefix every operation path is relative to: basePath in
    # 2.0, the path part of servers[0].url in 3.x. The /unifier filter is
    # applied to the JOINED path — with basePath: /unifier the paths
    # become /, /get_asof_date, ... and filtering the raw path would
    # print nothing.
    if flavor == "swagger2":
        base = doc.get("basePath", "")
        return base if isinstance(base, str) else ""
    servers = doc.get("servers")
    if isinstance(servers, list) and servers and isinstance(servers[0], dict):
        return urlparse(servers[0].get("url", "") or "").path
    return ""


def _resolve_ref(doc: Any, ref: Any) -> Dict[str, Any]:
    # Generic JSON-pointer walk: #/definitions/X, #/components/schemas/X,
    # #/$defs/X — any #/a/b/c, ~0/~1 escaped. Unresolvable refs return {}
    # (the caller falls back to the ref's own name), never raise.
    if not isinstance(ref, str) or not ref.startswith("#/"):
        return {}
    node: Any = doc
    for token in ref[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(node, dict) and token in node:
            node = node[token]
        else:
            return {}
    return node if isinstance(node, dict) else {}


def _field_type(fdef: Dict[str, Any]) -> str:
    # Tolerant of every variant: type as a string or a list (3.1 JSON
    # Schema 2020-12), nullable (3.0), $ref-typed fields, format and enum
    # adornments, array items and 3.2's itemSchema.
    t = fdef.get("type")
    if isinstance(t, list):
        rendered = "|".join(str(v) for v in t)
    elif isinstance(t, str):
        rendered = t
    else:
        ref = fdef.get("$ref", "")
        rendered = ref.rsplit("/", 1)[-1] if ref else "?"
    if fdef.get("nullable"):
        rendered += "|null"
    if rendered.split("|", 1)[0] == "array":
        items = fdef.get("items") or fdef.get("itemSchema") or {}
        if isinstance(items, dict) and items:
            inner = items.get("type")
            if isinstance(inner, str):
                rendered += f" of {inner}"
            elif isinstance(inner, list):
                rendered += " of " + "|".join(str(v) for v in inner)
            elif items.get("$ref"):
                rendered += f" of {items['$ref'].rsplit('/', 1)[-1]}"
    enum = fdef.get("enum")
    if isinstance(enum, list) and enum:
        rendered += f" ({'|'.join(str(v) for v in enum)})"
    elif isinstance(fdef.get("format"), str):
        rendered += f" ({fdef['format']})"
    return rendered


def _op_body_schema(op: Dict[str, Any], swagger2: bool) -> Dict[str, Any]:
    # Where the body schema lives is the only version branch: a
    # parameter with in: body (2.0) vs requestBody.content (3.x).
    if swagger2:
        for p in op.get("parameters", []) or []:
            if isinstance(p, dict) and p.get("in") == "body":
                schema = p.get("schema")
                return schema if isinstance(schema, dict) else {}
        return {}
    rb = op.get("requestBody")
    if not isinstance(rb, dict):
        return {}
    content = rb.get("content")
    if not isinstance(content, dict):
        return {}
    media = content.get("application/json")
    schema = media.get("schema") if isinstance(media, dict) else None
    if schema is None:
        for v in content.values():
            if isinstance(v, dict) and "schema" in v:
                schema = v["schema"]
                break
    return schema if isinstance(schema, dict) else {}


def _schema_properties(doc: Any, schema: Dict[str, Any]):
    # Merge properties across $ref and allOf/oneOf/anyOf branches (swaggo
    # wraps a $ref in allOf with its own sibling description); returns
    # (model name, properties). Unresolvable refs contribute nothing.
    props: Dict[str, Any] = {}
    name = None

    def collect(s: Any) -> None:
        nonlocal name
        if not isinstance(s, dict):
            return
        ref = s.get("$ref", "")
        if ref:
            name = ref.rsplit("/", 1)[-1]
            s = _resolve_ref(doc, ref) or s
        p = s.get("properties")
        if isinstance(p, dict):
            props.update(p)
        for combiner in ("allOf", "oneOf", "anyOf"):
            subs = s.get(combiner)
            if isinstance(subs, list):
                for sub in subs:
                    collect(sub)

    collect(schema)
    return name, props


def _render_help(doc: Dict[str, Any], flavor: str) -> None:
    base = _base_prefix(doc, flavor)
    swagger2 = flavor == "swagger2"
    paths = doc.get("paths")
    for path, ops in sorted((paths or {}).items()):
        joined = (base.rstrip("/") + path) if base else path
        if not joined.startswith("/unifier"):
            continue
        if not isinstance(ops, dict):
            continue
        for method, op in sorted(ops.items()):
            if method in _PATH_ITEM_NON_OPS or not isinstance(op, dict):
                continue
            print(f"{method.upper()} {joined} — {op.get('summary', '')}")
            if op.get("description") and op["description"] != op.get("summary"):
                print(f"  {op['description']}")
            name, props = _schema_properties(doc, _op_body_schema(op, swagger2))
            if props:
                print(f"  body ({name or 'inline'}):")
                for fname, fdef in sorted(props.items()):
                    fdef = fdef if isinstance(fdef, dict) else {}
                    ftype = _field_type(fdef)
                    fdesc = fdef.get("description", "")
                    print(f"    {fname}: {ftype}" + (f" — {fdesc}" if fdesc else ""))
            for p in op.get("parameters", []) or []:
                if not isinstance(p, dict) or p.get("in") == "body":
                    continue
                if p.get("name"):
                    ptype = p.get("type")
                    if not isinstance(ptype, str):
                        ptype = _field_type(p.get("schema") or {})
                    print(
                        f"  {p.get('in', '?')} {p['name']}: {ptype}"
                        + (f" — {p.get('description', '')}" if p.get("description") else "")
                    )


class Unifier:
    """Client helpers to interact with the Unifier API.

    Notes
    -----
    - Credentials can be set via the class variables `user` and `token` or
      passed per-call to the query helpers.
    - The client knows no query parameters: everything beyond the dataset
      name and credentials passes through to the server verbatim. The
      protocol is the contract, documented by the server's OpenAPI
      document — `Unifier.help()` fetches and prints it over the internet.
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

    # Prefer: wait budgets the CONNECTION too, not just the request: the
    # wait sent is the current backoff value minus the time the reused
    # connection has been open; when what is left drops below cycle_pct of
    # the value, the connection is closed after that request (Connection:
    # close) and the next one opens fresh at the full value.
    cycle_pct = 0.2
    _now = staticmethod(time.monotonic)
    _sleep = staticmethod(time.sleep)

    # A 429 or 503 on a long-poll GET (rate limiting, maintenance) is not
    # a backoff step: wait as the server's Retry-After says
    # (retry_after_default seconds when it says nothing) and re-request
    # at the same level. Such answers never reach the location, so they
    # do not keep it alive: a retry that would leave the location
    # untouched for longer than its idle lifetime (location_idle_ttl,
    # the server's QUERY_IDLE_TTL, 60s by default) minus
    # location_ttl_margin fails as a real error instead.
    location_idle_ttl = 60
    location_ttl_margin = 10
    retry_after_default = 1

    # Extra HTTP headers sent with every request — the query POST, the
    # long-poll GETs and cleanup DELETEs, the catalog calls, replicate
    # and help() — e.g. an Authorization header. They override the
    # client's own defaults (User-Agent, Content-Type).
    headers: Dict[str, str] = {}

    # Conflation flow control: the range-span budget is
    # bounded by a staleness target (seconds of delivery spread), and the
    # capacity estimate is an EWMA of bytes-per-target-second samples.
    STALENESS_TARGET = 1
    ALPHA = 0.25

    __version__ = '0.1.24'

    # Process-level default for the client_version field, read once at
    # import from UNIFIER_CLIENT_VERSION_OVERRIDE: unset -> this client's
    # version; "none" -> legacy mode process-wide (field omitted AND
    # []-on-failure, the no-code-changes escape hatch for pre-0.1.24
    # callers); any other string -> sent verbatim (useful for support and
    # staging to reproduce another client's behavior). An explicit
    # per-call client_version (string or None) always wins over this.
    _ENV_CLIENT_VERSION = os.environ.get("UNIFIER_CLIENT_VERSION_OVERRIDE")

    @classmethod
    def _resolve_client_version(cls, client_version: Any) -> Optional[str]:
        # _AUTO -> the process-level default (or this client's version);
        # anything a caller passed explicitly — a string, or None for
        # legacy mode — is returned unchanged. Resolution is idempotent,
        # so an already-resolved value passes through untouched.
        if client_version is not _AUTO_CLIENT_VERSION:
            return client_version
        env = cls._ENV_CLIENT_VERSION
        if env is None:
            return cls.__version__
        return None if env.strip().lower() == "none" else env

    @classmethod
    def _http_headers(cls) -> Dict[str, str]:
        # Every request identifies the client: unifier-python/<version>;
        # the caller's extra headers come last and win.
        return {
            "Content-Type": "application/json",
            "User-Agent": f"unifier-python/{cls.__version__}",
            **cls.headers,
        }

    _LEGACY_CATALOG_LOGS = {
        # 0.1.23's exact log texts, per endpoint: which error-body keys
        # counted, and the wording of each soft-fail line. The as-of
        # transport line is unprefixed, and its non-JSON bodies reached
        # the transport handler (a JSONDecodeError is a
        # RequestException), so they share that text.
        "as-of dates": {
            "detail_keys": ("error",),
            "error": "Unifier as-of dates error: {detail}",
            "status": "Unifier as-of dates request failed with status code"
                      " {status}",
            "failed": "As-of dates request failed: {error}",
            "shape": "Unexpected as-of dates response format: {type!r}",
        },
        "list-data-catalog": {
            "detail_keys": ("error",),
            "error": "Unifier list-data-catalog error: {detail}",
            "status": "Unifier list-data-catalog failed with status code"
                      " {status}",
            "failed": "Unifier list-data-catalog request failed: {error}",
            "shape": "Unexpected list-data-catalog response format: {type!r}",
        },
        "list-entitled-datasets": {
            "detail_keys": ("error", "detail"),
            "error": "Unifier list-entitled-datasets error: {detail}",
            "status": "Unifier list-entitled-datasets failed with status"
                      " code {status}",
            "failed": "Unifier list-entitled-datasets request failed:"
                      " {error}",
            "shape": "Unexpected list-entitled-datasets response format:"
                     " {type!r}",
        },
        "get-dataset-details": {
            "detail_keys": ("error",),
            "error": "Unifier get-dataset-details error: {detail}",
            "status": "Unifier get-dataset-details failed with status code"
                      " {status}",
            "failed": "Unifier get-dataset-details request failed: {error}",
            "shape": "Unexpected get-dataset-details response format:"
                     " {type!r}",
        },
    }

    @classmethod
    def _post_catalog(
        cls,
        _url: str,
        payload: Dict[str, Any],
        legacy_mode: bool,
        expect: Optional[str],
        legacy_check: bool,
        empty: Any,
        label: str,
    ) -> Any:
        # One POST with the unified error contract. With client_version
        # sent, every failure raises UnifierError: transport, a non-200
        # (surfacing the server's error text when the body carries one,
        # the status code otherwise), a 200 body that is not JSON, and a
        # 200 of the wrong shape (`expect`: "list" or "dict") — the
        # server validates the request; the client only interprets its
        # own answer. Legacy mode (client_version None, compatibility)
        # keeps 0.1.23's soft-fail: the method's empty error shape with
        # 0.1.23's exact log texts (_LEGACY_CATALOG_LOGS), and shape
        # checks only where 0.1.23 had them (`legacy_check`); a
        # wrong-shaped 200 passes through otherwise.
        headers = cls._http_headers()
        legacy_logs = cls._LEGACY_CATALOG_LOGS[label]
        try:
            response = requests.post(
                _url, headers=headers, **_post_body(payload, legacy_mode),
            )
            if response.status_code != 200:
                try:
                    err = response.json()
                except ValueError:
                    err = {}
                detail = None
                if isinstance(err, dict):
                    detail = next(
                        (err[k] for k in legacy_logs["detail_keys"] if err.get(k)),
                        None,
                    )
                if legacy_mode:
                    if detail:
                        message = legacy_logs["error"].format(detail=detail)
                    else:
                        message = legacy_logs["status"].format(
                            status=response.status_code,
                        )
                    logger.warning(message)
                    return empty
                if detail:
                    message = f"Unifier {label} error: {detail}"
                else:
                    message = (
                        f"Unifier {label} failed with status code"
                        f" {response.status_code}"
                    )
                raise UnifierError(message)
            try:
                data = response.json()
            except ValueError as e:
                if legacy_mode:
                    message = legacy_logs["failed"].format(error=e)
                    logger.error(message)
                    return empty
                raise UnifierError(
                    f"Unifier {label} request failed: {e}"
                ) from e
            if (expect == "list" and not isinstance(data, list)) or (
                expect == "dict" and not isinstance(data, dict)
            ):
                if legacy_mode and not legacy_check:
                    return data    # 0.1.23 passed wrong shapes through
                if legacy_mode:
                    message = legacy_logs["shape"].format(type=data)
                    logger.warning(message)
                    return empty
                raise UnifierError(
                    f"Unexpected {label} response format: {type(data)!r}"
                )
            return data
        except requests.exceptions.RequestException as e:
            if legacy_mode:
                message = legacy_logs["failed"].format(error=e)
                logger.error(message)
                return empty
            raise UnifierError(
                f"Unifier {label} request failed: {e}"
            ) from e

    @classmethod
    def get_asof_dates_query(
        cls,
        *legacy: Any,
        name: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:
        """Get a list of available as-of dates for a given data `name`.

        Parameters
        ----------
        name: str
            The dataset or query name for which to fetch as-of dates.
            Keyword-only; positional use is deprecated (warns).

        Returns
        -------
        list[dict]
            A flattened list of records with available as-of dates. Raises
            UnifierError on failure; with client_version=None
            (compatibility mode) returns an empty list instead, as 0.1.23 did.
        Credentials (`user`, `token`), `client_version`, and arbitrary
        protocol parameters (`params` and extra keyword arguments) ride to
        the server exactly like query()'s; see query().
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            name = _legacy_positional("get_asof_dates_query", ("name",), legacy, {"name": name})["name"]
        client_version = cls._resolve_client_version(client_version)
        payload = cls._payload(name, user, token, client_version, params, kw)
        data = cls._post_catalog(
            cls.url + "/get_asof_date", payload, client_version is None,
            expect="list", legacy_check=True, empty=[], label="as-of dates",
        )
        return [
            {k: v for d in item for k, v in d.items()} for item in data
        ]

    @classmethod
    def get_asof_dates(
        cls,
        *legacy: Any,
        name: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
    ) -> pd.DataFrame:
        """Get a pandas DataFrame of available as-of dates for a given name.

        `name` is keyword-only; positional use is deprecated (warns).
        Credentials (`user`, `token`), `client_version`, and arbitrary
        protocol parameters (`params` and extra keyword arguments) ride to
        the server exactly like query()'s; see query().
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            name = _legacy_positional("get_asof_dates", ("name",), legacy, {"name": name})["name"]
        _data = cls.get_asof_dates_query(
            name=name, user=user, token=token,
            client_version=client_version, params=params, **kw,
        )
        return pd.DataFrame(_data)

    @classmethod
    def get_asof_dates_json(
        cls,
        *legacy: Any,
        name: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:
        """Get a Python list of available as-of dates for a given name.

        `name` is keyword-only; positional use is deprecated (warns).
        Credentials (`user`, `token`), `client_version`, and arbitrary
        protocol parameters (`params` and extra keyword arguments) ride to
        the server exactly like query()'s; see query().
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            name = _legacy_positional("get_asof_dates_json", ("name",), legacy, {"name": name})["name"]
        return cls.get_asof_dates_query(
            name=name, user=user, token=token,
            client_version=client_version, params=params, **kw,
        )

    @classmethod
    def list_data_catalog(
        cls,
        *legacy: Any,
        show_all_datasets: bool = False,
        product_type: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
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
            A list of dataset metadata objects. Raises UnifierError on
            failure; with client_version=None (compatibility mode) returns
            an empty list instead, as 0.1.23 did.
        Parameters are keyword-only; positional use is deprecated (warns).
        Credentials (`user`, `token`), `client_version`, and arbitrary
        protocol parameters (`params` and extra keyword arguments) ride to
        the server exactly like query()'s; see query().
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            a = _legacy_positional(
                "list_data_catalog", ("show_all_datasets", "product_type"), legacy,
                {"show_all_datasets": show_all_datasets, "product_type": product_type},
                defaults={"show_all_datasets": False},
            )
            show_all_datasets, product_type = a["show_all_datasets"], a["product_type"]
        client_version = cls._resolve_client_version(client_version)
        legacy_mode = client_version is None
        payload = cls._payload(None, user, token, client_version, params, kw)
        _apply_catalog_scope(
            payload, show_all_datasets, product_type, warn=legacy_mode,
        )
        return cls._post_catalog(
            cls.url + "/list-data-catalogs", payload, legacy_mode,
            expect="list", legacy_check=False, empty=[], label="list-data-catalog",
        )

    @classmethod
    def list_entitled_datasets(
        cls,
        *legacy: Any,
        date: Optional[Any] = None,
        from_date: Optional[Any] = None,
        to_date: Optional[Any] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
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
            Dataset metadata objects sorted by `unifier_source_name`. Raises
            UnifierError on failure; with client_version=None
            (compatibility mode) returns an empty list instead, as 0.1.23 did.

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
        Parameters are keyword-only; positional use is deprecated (warns).
        Credentials (`user`, `token`), `client_version`, and arbitrary
        protocol parameters (`params` and extra keyword arguments) ride to
        the server exactly like query()'s; see query().
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            a = _legacy_positional(
                "list_entitled_datasets", ("date", "from_date", "to_date"), legacy,
                {"date": date, "from_date": from_date, "to_date": to_date},
            )
            date, from_date, to_date = a["date"], a["from_date"], a["to_date"]
        client_version = cls._resolve_client_version(client_version)
        legacy_mode = client_version is None
        payload = cls._payload(None, user, token, client_version, params, kw)
        # The date names are client-owned in both modes: 0.1.23's
        # client-side validation and normalization, verbatim — the values
        # sent are always plain YYYY-MM-DD strings, whatever type the
        # caller passed, and conflicting or unparseable dates are
        # rejected before any request.
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
        # Omit unset date fields entirely so the server applies its own default.
        if _date:
            payload["date"] = _date
        if _from_date:
            payload["from_date"] = _from_date
        if _to_date:
            payload["to_date"] = _to_date
        return cls._post_catalog(
            cls.url + "/list-entitled-datasets", payload, legacy_mode,
            expect="list", legacy_check=False, empty=[], label="list-entitled-datasets",
        )

    @classmethod
    def get_dataset_details(
        cls,
        *legacy: Any,
        name: Optional[str] = None,
        show_all_datasets: bool = False,
        product_type: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
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
            The dataset metadata object. Raises UnifierError on failure;
            with client_version=None (compatibility mode) returns an empty
            dict instead, as 0.1.23 did.
        Parameters are keyword-only; positional use is deprecated (warns).
        Credentials (`user`, `token`), `client_version`, and arbitrary
        protocol parameters (`params` and extra keyword arguments) ride to
        the server exactly like query()'s; see query().
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            a = _legacy_positional(
                "get_dataset_details", ("name", "show_all_datasets", "product_type"), legacy,
                {"name": name, "show_all_datasets": show_all_datasets, "product_type": product_type},
                defaults={"show_all_datasets": False},
            )
            name, show_all_datasets, product_type = a["name"], a["show_all_datasets"], a["product_type"]
        client_version = cls._resolve_client_version(client_version)
        legacy_mode = client_version is None
        payload = cls._payload(name, user, token, client_version, params, kw)
        _apply_catalog_scope(
            payload, show_all_datasets, product_type, warn=legacy_mode,
        )
        return cls._post_catalog(
            cls.url + "/data-catalog", payload, legacy_mode,
            expect="dict", legacy_check=False, empty={}, label="get-dataset-details",
        )

    @classmethod
    def help(cls) -> None:
        """Print the query protocol's parameters, fetched live from the server.

        The client bakes in no parameter knowledge, so the documentation
        cannot ship with it either: this fetches the server's API document
        at <url>/swagger/doc.json over the internet and prints the request
        model of every /unifier path it describes. Swagger 2.0 and OpenAPI
        3.x (3.0 through 3.2, served as JSON) are rendered; any openapi
        3.x is handled with the 3.1 rules, since 3.2 only adds. Anything
        the renderer cannot make sense of — an unknown version, a
        non-JSON document, an unexpected structure — prints the URL and
        the raw document instead of raising. Only a fetch failure raises
        UnifierError naming the URL.
        """
        doc_url = cls.url.rstrip("/") + "/swagger/doc.json"
        try:
            response = requests.get(
                doc_url,
                timeout=10,
                headers={"User-Agent": f"unifier-python/{cls.__version__}", **cls.headers},
            )
            response.raise_for_status()
            raw = response.text
        except Exception as e:
            raise UnifierError(f"cannot fetch the protocol document from {doc_url}: {e}") from e
        try:
            doc = json.loads(raw)
            flavor = _doc_flavor(doc)
            if flavor is None:
                raise ValueError(f"unrecognized document version")
            _render_help(doc, flavor)
        except UnifierError:
            raise
        except Exception:
            print(f"unifier-python cannot render the API document at {doc_url}; here it is verbatim:")
            print(raw)

    @classmethod
    def query(
        cls,
        *legacy: Any,
        name: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
    ) -> List[Sequence[Dict[str, Any]]]:
        """Query the Unifier API and return the raw response data as-is.

        The client knows no query parameters: everything beyond `name` and
        credentials rides verbatim to the server, from `params` and any extra
        keyword arguments alike. The protocol is the contract, documented by
        the server's OpenAPI document — `Unifier.help()` fetches and prints
        it. This helper must not need a release per new server parameter.

        All parameters are keyword-only. Positional arguments are deprecated:
        the positional orders of earlier releases still work but warn
        (FutureWarning) with the equivalent named call, and support will be
        removed in a future release.

        Parameters
        ----------
        name : Optional[str]
            The query or dataset name; sent only when given.
        user, token : Optional[str]
            Override class-level credentials.
        client_version : Optional[str]
            Client version string sent with the request. Defaults to this
            client's actual version, or to UNIFIER_CLIENT_VERSION_OVERRIDE
            when that environment variable is set at import ("none" means
            legacy mode: field omitted and errors soft-fail); a
            caller-supplied string is sent as-is; explicit None sends no
            client_version field.
        params : Optional[Dict[str, Any]]
            Protocol parameters, merged into the request payload verbatim.
            Extra keyword arguments do the same (kw wins on collision).
            By default query() sends no `conflation` field. Passing one
            (any value, None included) makes the request a stream, served
            like stream()'s; query() returns once the server ends it, so
            only use it for streaming queries that end.

        Returns
        -------
        list
            The JSON payload returned by the API (commonly a list of lists of
            dicts).

        Raises
        ------
        UnifierError
            Any failure — transport, a non-200 answer (surfacing the
            server's error text when the body carries one, the status
            code otherwise), a 200 body that is not JSON, or a 200 that
            is JSON but not a list — when client_version is sent (the
            default). With parameters passing through verbatim, the
            server is the only parameter speller the request will ever
            meet; swallowing its rejection into [] would make a typo
            indistinguishable from a legitimate empty result.
            client_version=None (legacy mode) keeps the pre-0.1.24
            contract instead: log and return [].
        """
        if legacy:
            name, user, token, kw = cls._legacy_query_args("query", legacy, name, user, token, kw)

        client_version = cls._resolve_client_version(client_version)
        legacy_mode = client_version is None
        # `as_of` is 0.1.23's deprecated name, keyword form only: a
        # params-dict as_of is an ordinary parameter (verbatim, unwarned),
        # and the positional form was already rebound to asof_date by
        # _legacy_query_args. With client_version sent, a keyword as_of
        # rides as itself AND — unless asof_date is also given (present
        # and not None) — is mirrored to asof_date with the same
        # uniformly encoded value, so a server that reads only asof_date
        # (main) still sees it. client_version=None sends 0.1.23's wire:
        # asof_date only.
        if "as_of" in kw:
            _warn_deprecated(
                "unifier: 'as_of' is deprecated; use 'asof_date'", 2,
            )
            if legacy_mode:
                as_of = kw.pop("as_of")
                if as_of is not None and kw.get("asof_date") is None:
                    kw["asof_date"] = as_of
            elif kw["as_of"] is not None and kw.get("asof_date") is None:
                kw["asof_date"] = kw["as_of"]
        if legacy_mode:
            payload = cls._legacy_query_payload(name, user, token, params, kw)
        else:
            payload = cls._payload(name, user, token, client_version, params, kw)

        stream = cls._body_stream(payload, legacy_mode)
        try:
            body = b"".join(stream)
        except _BodyError as e:
            # The transport already logged the failure (and sent its
            # cleanup DELETE for the long-poll path).
            if legacy_mode:
                return []
            raise UnifierError(e.message) from e
        try:
            response_data = json.loads(body)
        except ValueError as e:
            # Delivered bytes are not JSON: same outcome and log as ever,
            # plus post-202 cleanup when the body came from a Location.
            message = f"Unifier query request failed: {e}"
            if stream.location is not None:
                cls._cleanup_delete(stream.session, stream.location, message)
            else:
                logger.error(message)
            if legacy_mode:
                return []
            raise UnifierError(message) from e
        if not isinstance(response_data, list):
            message = f"Unexpected query response format: {type(response_data)!r}"
            if stream.location is not None:
                cls._cleanup_delete(stream.session, stream.location, message)
            else:
                logger.warning(message)
            if legacy_mode:
                return []
            raise UnifierError(message)
        return response_data

    @staticmethod
    def _legacy_query_args(method, legacy, name, user, token, kw):
        # Deprecated positional call in the pre-0.1.24 order of query(),
        # get_dataframe() and get_json(): bind it to names (and warn, at the
        # caller's line: user -> method -> here -> _legacy_positional).
        # A positional as_of (position 6) meant asof_date, unless asof_date
        # (position 9) was also given: rebind it so the suggested named call
        # uses the current name.
        legacy = list(legacy)
        if len(legacy) > 5 and legacy[5] is not None:
            if len(legacy) <= 8 or legacy[8] is None:
                legacy += [None] * (9 - len(legacy))
                legacy[8] = legacy[5]
            legacy[5] = None
        a = _legacy_positional(
            method, _LEGACY_QUERY_ORDER, legacy,
            {"name": name, "user": user, "token": token, **kw}, stacklevel=4,
        )
        name, user, token = a.pop("name"), a.pop("user"), a.pop("token")
        return name, user, token, a

    @classmethod
    def _payload(
        cls,
        name: Optional[str],
        user: Optional[str],
        token: Optional[str],
        client_version: Optional[str],
        params: Optional[Dict[str, Any]],
        kw: Dict[str, Any],
    ) -> Dict[str, Any]:
        # The wire payload, shared by query(), stream() and replicate().
        # The client knows no query parameters: everything beyond name and
        # credentials rides verbatim from `params` and keyword arguments (kw
        # wins over params on collision). The protocol is the contract,
        # documented by the server (help() fetches its OpenAPI document);
        # this client is a convenience helper and must not need a release
        # per new server parameter. Helper-owned fields (name, credentials,
        # client_version) always win: they are set from the helper's own
        # arguments after the merge (name only when given: a future request
        # may name its datasets otherwise).
        payload: Dict[str, Any] = dict(params or {})
        payload.update(kw)
        if name is not None:
            payload["name"] = name
        client_version = cls._resolve_client_version(client_version)
        # Credentials ride only when set (a call value, else the class
        # value): with authentication in a header (see `headers`), no
        # empty user/token fields are sent. client_version=None sends
        # them always, as 0.1.23 did.
        for field, value in (
            ("user", user if user is not None else cls.user),
            ("token", token if token is not None else cls.token),
        ):
            if value or client_version is None:
                payload[field] = value
            else:
                payload.pop(field, None)
        if client_version is not None:
            payload["client_version"] = client_version
        else:
            payload.pop("client_version", None)
        return payload

    @staticmethod
    def _legacy_reject(method: str, params: Any, kw: Dict[str, Any], allowed: set) -> None:
        # client_version=None speaks 0.1.23, whose signatures knew only
        # the frozen names: params did not exist, and a name it did not
        # know was a TypeError. Sending anything else would be a wire
        # variation, so the same rejection stands.
        if params is not None:
            raise TypeError(
                f"{method}() accepts no params in client_version=None mode"
                " (0.1.23 compatibility)"
            )
        unknown = set(kw) - allowed
        if unknown:
            raise TypeError(
                f"{method}() got an unexpected keyword argument"
                f" {sorted(unknown)[0]!r} (client_version=None mode is"
                " 0.1.23 compatibility)"
            )

    @classmethod
    def _legacy_query_payload(
        cls,
        name: Optional[str],
        user: Optional[str],
        token: Optional[str],
        params: Any,
        kw: Dict[str, Any],
    ) -> Dict[str, Any]:
        # 0.1.23's query wire, exactly: name, class credentials (a
        # non-None call overrides) and disable_view always — even False,
        # even an explicit None — then the frozen optional names only
        # when not None, keys as list(keys) whatever it is. as_of was
        # already rebound to asof_date by the caller (an explicit
        # non-None asof_date wins, as in 0.1.23).
        cls._legacy_reject(
            "query", params, kw,
            {"key", "keys", "back_to", "up_to", "asof_date", "asof_back_to",
             "limit", "column_filters", "disable_view"},
        )
        payload: Dict[str, Any] = {
            "name": name,
            "user": cls.user,
            "token": cls.token,
            "disable_view": kw.get("disable_view", False),
        }
        if user is not None:
            payload["user"] = user
        if token is not None:
            payload["token"] = token
        for key in ("key", "keys", "back_to", "up_to", "asof_date",
                    "asof_back_to", "limit", "column_filters"):
            if kw.get(key) is not None:
                payload[key] = kw[key]
        if payload.get("keys") is not None:
            payload["keys"] = list(payload["keys"])
        return payload

    @classmethod
    def _legacy_replicate_payload(
        cls,
        name: Optional[str],
        user: Optional[str],
        token: Optional[str],
        params: Any,
        kw: Dict[str, Any],
    ) -> Dict[str, Any]:
        # 0.1.23's replicate wire, exactly: name and class credentials,
        # and the three date names only when truthy. bandwidth_limit,
        # use_rclone and target_location never reached the wire.
        cls._legacy_reject(
            "replicate", params, kw, {"asof_date", "back_to", "up_to"},
        )
        payload: Dict[str, Any] = {
            "name": name,
            "user": cls.user,
            "token": cls.token,
        }
        if user is not None:
            payload["user"] = user
        if token is not None:
            payload["token"] = token
        for key in ("asof_date", "back_to", "up_to"):
            if kw.get(key):
                payload[key] = kw[key]
        return payload

    @classmethod
    def _body_stream(cls, payload: Dict[str, Any], legacy_mode: bool = False) -> _BodyStream:
        # The body-byte transport for query() and stream(): one stream that
        # owns the 200/202 dispatch (see _BodyStream). legacy_mode encodes
        # the POST body as 0.1.23 did (see _post_body).
        return _BodyStream(cls, payload, legacy_mode)

    @classmethod
    def _wait_sequence(cls) -> Sequence[int]:
        if cls.wait is None:
            return cls.BACKOFF_WAITS
        return (cls.wait,) + tuple(v for v in cls.BACKOFF_WAITS if v < cls.wait)

    @classmethod
    def _cleanup_delete(cls, session, location: str, message: str) -> None:
        # Real error post-202: best-effort DELETE cleanup on the query's
        # own session (the long-poll connection is reused when possible).
        # We do not care whether it succeeds; a failed DELETE is appended
        # info on the error we already have, never a new error. The
        # server's own cleanup does not strictly depend on this DELETE
        # arriving.
        cleanup = ""
        try:
            response = session.delete(location)
            if not 200 <= response.status_code < 300:
                cleanup = (
                    f"; cleanup DELETE failed with status code"
                    f" {response.status_code}"
                )
        except (requests.exceptions.RequestException,
                urllib3.exceptions.HTTPError) as e:
            cleanup = f"; cleanup DELETE failed: {e}"
        logger.warning("%s%s", message, cleanup)

    @classmethod
    def _range_poll(cls, stream: "_BodyStream", location: str) -> Iterator[bytes]:
        # Long-poll a 202-formed response: open-ended Range GETs from the
        # next unread byte, with Prefer: wait=<seconds> so the server can
        # hold each request until more bytes exist. The GETs ride the
        # query's requests session (requests' adapter makes no retries:
        # this loop owns retrying) and send Accept-Encoding: identity
        # explicitly — the range cursor is byte-level, and requests'
        # default advertisement would invite compression from the server
        # or any proxy that the cursor arithmetic cannot consume — and
        # yield the 206 body as it streams: a transport failure (not an
        # HTTP error) that
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
        # The wait value also budgets the reused connection's age: it is
        # the backoff value minus how long the connection has been open,
        # and below cycle_pct of the value the request closes the
        # connection (Connection: close) so the next opens fresh.
        # Yields the assembled body's bytes; the consuming front joins
        # and interprets them through the same path as a 200 response.
        session = stream.session
        seq = cls._wait_sequence()
        level = cls._wait_level
        offset = 0
        # Seconds spent waiting on 429/503 since the location last heard
        # from us (the 202, then every 204/206 — an answer from the
        # location itself). Those waits are the only time the client
        # leaves the location untouched on purpose; the requests between
        # them are short, and location_ttl_margin covers them.
        untouched = 0.0
        # Conflation flow control: when the payload carries a
        # conflation field (any value, null included), ranges are bounded with a
        # per-query span budget — seeded at 1 byte, so the first response
        # is exactly one whole message — growing under delivery pressure,
        # shrinking on staleness violations, with an EWMA capacity
        # estimate. Off: the open-ended range, unchanged.
        conflation = "conflation" in stream.payload
        span = 1 if conflation else None
        capacity = None
        while True:
            # The budget is the current backoff value; the wait sent is
            # what remains of it on this connection. The connection-age
            # clock restarts whenever the connection is known dead (our
            # cycle close, an HTTP error close, or a transport failure).
            budget = seq[level]
            remaining = budget - (cls._now() - stream.conn_epoch)
            wait = max(1, int(remaining))
            cycle = wait < budget * cls.cycle_pct
            if conflation:
                headers = {
                    "Range": f"bytes={offset}-{offset + span - 1}",
                    "Prefer": f"wait={wait}",
                    "Accept-Encoding": "identity",
                }
            else:
                headers = {
                    "Range": f"bytes={offset}-",
                    "Prefer": f"wait={wait}",
                    "Accept-Encoding": "identity",
                }
            if cycle:
                headers["Connection"] = "close"
            http_detail = None
            transport_detail = None
            response = None
            received = 0
            total = None
            try:
                response = session.get(location, headers=headers, stream=True)
            except requests.exceptions.RequestException as e:
                transport_detail = str(e)
                # The connection is dead; the next request opens a fresh
                # one, so the age clock restarts.
                stream.conn_epoch = cls._now()

            if response is not None and response.status_code == 204:
                # Nothing new yet: ignore and re-request the same range
                # cursor. The loop builds each Range from `offset`, which
                # only advances when bytes arrive, so `continue` re-asks
                # for the same open-ended byte range. 204 has no body by
                # definition: drain (no-op read) and return the clean
                # connection to the pool for the next GET to reuse.
                response.content
                untouched = 0.0
                if cycle:
                    session.close()
                    stream.conn_epoch = cls._now()
                continue

            if response is not None and response.status_code in (429, 503):
                # Rate limited or unavailable: wait as told, then retry
                # the same request at the same level — unless that would
                # outlive the location (see location_idle_ttl).
                delay = _retry_after(
                    response.headers.get("Retry-After"), cls.retry_after_default,
                )
                status = response.status_code
                response.close()
                if untouched + delay <= cls.location_idle_ttl - cls.location_ttl_margin:
                    cls._sleep(delay)
                    untouched += delay
                    # The answer's connection is closed: the next request
                    # opens a fresh one, so its age starts now.
                    stream.conn_epoch = cls._now()
                    continue
                message = (
                    f"Unifier query long-poll failed with status code {status}:"
                    f" retrying after {delay:g}s would outlive the location"
                )
                cls._wait_level = level
                cls._cleanup_delete(session, location, message)
                raise _BodyError(message)

            if response is not None and response.status_code != 206:
                # HTTP-level error: no body is collected from it, and the
                # connection is closed — the clock restarts for the retry.
                http_detail = f"status code {response.status_code}"
                response.close()
                stream.conn_epoch = cls._now()
            elif response is not None:
                # 206. Trust the Content-Range the server reports.
                untouched = 0.0
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
                # In conflation mode, the first and last data-read times
                # are the delivery spread's bounds; the hold before the
                # first byte never counts.
                first_byte = None
                last_byte = None
                try:
                    while True:
                        piece = response.raw.read1(65536)
                        if not piece:
                            break
                        if conflation:
                            now = cls._now()
                            if first_byte is None:
                                first_byte = now
                            last_byte = now
                        yield piece
                        received += len(piece)
                except urllib3.exceptions.HTTPError as e:
                    transport_detail = str(e)
                    stream.conn_epoch = cls._now()  # connection is dead
                finally:
                    response.close()
                if conflation and received:
                    # Flow-control update: exactly once
                    # per 206 that delivered bytes — complete or cut —
                    # before the next request's range is formed. A cut that
                    # delivered no bytes never reaches here (received == 0):
                    # no sample, no update.
                    spread = last_byte - first_byte
                    measured = spread >= cls.STALENESS_TARGET / 2
                    if measured:
                        sample = received * cls.STALENESS_TARGET / spread
                        if capacity is None:
                            capacity = sample
                        else:
                            capacity += cls.ALPHA * (sample - capacity)
                    if spread > cls.STALENESS_TARGET:
                        # Staleness violation: halve, but never below the
                        # measured capacity (a no-op when capacity already
                        # covers the span).
                        span = min(span, max(capacity, span / 2))
                    elif 2 * received > span:
                        limit = capacity if measured else float("inf")
                        span = max(span, min(limit, 2 * received))
                    span = max(int(span), 1)
                if transport_detail is None:
                    # The whole declared chunk arrived.
                    offset = end + 1
                    if cycle:
                        session.close()
                        stream.conn_epoch = cls._now()
                    if total is not None and offset >= total:
                        break
                    continue
                # Transport cut mid-body: the bytes read are data; advance
                # the cursor past them for the retry. If they already reach
                # the reported total, the cut lost nothing: complete.
                offset += received
                if total is not None and offset >= total:
                    break

            # An error (HTTP-level or transport): back off one wait level
            # and retry, or fail for real at the minimum wait.
            if level + 1 < len(seq):
                level += 1
                cls._clean_queries = 0
                continue
            cls._wait_level = level
            message = (
                f"Unifier query long-poll failed with"
                f" {transport_detail or http_detail}"
                f" at minimum wait {seq[level]}"
            )
            cls._cleanup_delete(session, location, message)
            raise _BodyError(message)
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
        *legacy: Any,
        name: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
    ) -> pd.DataFrame:
        """Get the query result as a pandas DataFrame.

        Takes query()'s parameters (all protocol parameters pass through
        verbatim; all keyword-only, positional use deprecated; see query()).
        Returns an empty DataFrame for an empty result. Failures raise
        UnifierError, as query()'s do; with client_version=None
        (compatibility mode) they return an empty DataFrame, as in 0.1.23.
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            name, user, token, kw = cls._legacy_query_args("get_dataframe", legacy, name, user, token, kw)

        json_result = cls.query(
            name=name,
            user=user,
            token=token,
            client_version=client_version,
            params=params,
            **kw,
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
        *legacy: Any,
        name: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:
        """Get the query result as a flattened JSON list of dicts.

        Takes query()'s parameters (all protocol parameters pass through
        verbatim; all keyword-only, positional use deprecated; see query()).
        Returns an empty list for an empty result. Failures raise
        UnifierError, as query()'s do; with client_version=None
        (compatibility mode) they return an empty list, as in 0.1.23.
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            name, user, token, kw = cls._legacy_query_args("get_json", legacy, name, user, token, kw)

        json_result = cls.query(
            name=name,
            user=user,
            token=token,
            client_version=client_version,
            params=params,
            **kw,
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
        *,
        name: Optional[str] = None,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        **kw: Any,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Stream the query result row by row as it arrives.

        Same parameters (keyword-only; stream() never accepted positional
        arguments) and same transport as query() (including the 202
        long-poll path and its salvage/backoff machinery), but rows are
        yielded as soon as each one completes instead of waiting for the
        whole body: a front can watch the response roll in as the server
        publishes it. Parsing is incremental (ijson, yajl2_c C backend
        confirmed at import: no fallback) and non-integer numbers are
        floats, matching json.loads, so rows carry the same value types as
        query() — except that integers must fit in 64 bits: a larger one
        fails the stream with UnifierError (the C backend's limit), where
        query() would return a Python int.

        Each yielded row is the row's list of fragment dicts, exactly as
        query() returns them; merge fragments as get_dataframe does if a
        flat record is wanted. There is no yield before the first complete
        row.

        The one piece of protocol knowledge the client bakes in: the
        `conflation` field is ALWAYS present in a stream() payload (query()
        omits it unless passed), so its presence marks the request as a
        stream to the server. The value names the conflation mode — a
        server-side detail; pass conflation=<mode> like any other
        parameter. Not passed, it is sent as null (JSON null: present,
        server's default mode). The POST must be answered 202 — the conflated
        response forms at a Location like any long-polled response; a
        direct 200 is an error.

        Yields
        ------
        list[dict]
            One row (a list of fragment dicts) per yield.

        Raises
        ------
        UnifierError
            Any failure, uniformly: the body transport failed (already
            logged; the long-poll path also sent its cleanup DELETE), a
            conflation request was answered 200 instead of 202, the
            response root is not the query list (a conforming server
            always sends the list; items() alone would silently yield
            nothing for other roots), or the delivered bytes ended
            before the JSON document did. The specific cause is in the
            message and the chained exception.

        Abandoning the generator mid-stream sends the cleanup DELETE to a
        long-polled location, just like any other incomplete query.
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        payload = cls._payload(name, user, token, client_version, params, kw)
        payload.setdefault("conflation", None)
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
            probe = _BodyFile(pieces, snapshot=True)
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
        except _BodyError as e:
            # Transport real error: already logged and DELETEd.
            raise UnifierError(e.message) from e
        except Exception as e:
            # Interpretation/spec failure (root violation, ijson
            # truncation): the location is an incomplete or unusable
            # resource — clean it up, then surface one public type.
            body.abandon()
            raise UnifierError(f"Unifier stream failed: {e}") from e
        finally:
            # Unwind the transport generator promptly no matter how the
            # stream ends: it closes its in-flight response; its own
            # cleanup sees a cleared location, so it cannot delete twice.
            pieces.close()

    @classmethod
    def replicate(
        cls,
        *legacy: Any,
        name: Optional[str] = None,
        target_location: str = _DEFAULT_TARGET,
        user: Optional[str] = None,
        token: Optional[str] = None,
        client_version: Optional[str] = _AUTO_CLIENT_VERSION,
        params: Optional[Dict[str, Any]] = None,
        bandwidth_limit: Optional[int] = None,
        use_rclone: bool = True,
        **kw: Any,
    ) -> None:
        """Replicate data to a local target location using rclone or native python.

        Like query(), the client knows no protocol parameters: everything
        beyond `name`, credentials and the local options below rides
        verbatim to the server, from `params` and any extra keyword
        arguments alike (kw wins on collision).

        All parameters are keyword-only. Positional arguments are deprecated:
        the positional order of earlier releases still works but warns
        (FutureWarning) with the equivalent named call, and support will be
        removed in a future release.

        Parameters
        ----------
        name : Optional[str]
            The dataset name to replicate; sent only when given.
        target_location : str
            The local directory path where data should be downloaded.
            Defaults to the current directory.
        user, token : Optional[str]
            Override class-level credentials.
        client_version : Optional[str]
            As for query(): defaults to this client's actual version.
        params : Optional[Dict[str, Any]]
            Protocol parameters, merged into the request payload verbatim.
        bandwidth_limit : Optional[int]
            The bandwidth limit in MB/s for the replication process (rclone only).
        use_rclone : bool
            If True, attempt to use rclone. Falls back to native Python implementation
            if rclone is not available.
        Protocol parameters and the live contract: see
        `Unifier.help()` (fetched from the server's OpenAPI document).
        """
        if legacy:
            # Pre-0.1.24 order; its dates were sent only when truthy, so a
            # falsy positional date means "not given".
            legacy = tuple(
                None if 2 <= i <= 4 and not v else v for i, v in enumerate(legacy)
            )
            a = _legacy_positional(
                "replicate", _LEGACY_REPLICATE_ORDER, legacy,
                {"name": name, "target_location": target_location, "user": user,
                 "token": token, "bandwidth_limit": bandwidth_limit,
                 "use_rclone": use_rclone, **kw},
                defaults={"use_rclone": True, "target_location": _DEFAULT_TARGET},
            )
            name, target_location, user, token = (
                a.pop("name"), a.pop("target_location"), a.pop("user"), a.pop("token"))
            bandwidth_limit, use_rclone = a.pop("bandwidth_limit"), a.pop("use_rclone")
            kw = a
        if target_location is None:   # an explicit None: the default
            target_location = _DEFAULT_TARGET
        client_version = cls._resolve_client_version(client_version)
        legacy_mode = client_version is None
        headers = cls._http_headers()
        if legacy_mode:
            # 0.1.23's wire: name, credentials, and the truthy dates only.
            payload = cls._legacy_replicate_payload(name, user, token, params, kw)
        else:
            payload = cls._payload(name, user, token, client_version, params, kw)

        # Check for rclone availability
        rclone_path = shutil.which("rclone")
        using_rclone = use_rclone and (rclone_path is not None)

        _url = cls.url + "/replicate"

        try:
            # Fetch replication credentials
            response = requests.post(
                _url, headers=headers, **_post_body(payload, legacy_mode),
            )
            if response.status_code != 200:
                try:
                    err_msg = response.json().get("error", "Unknown error")
                except ValueError:
                    err_msg = response.text
                err_msg = f"Unifier replicate error: {err_msg}"
                if legacy_mode:
                    logger.warning(err_msg)
                    print(err_msg)
                    return
                raise UnifierError(err_msg)

            try:
                resp_json = response.json()
            except ValueError as e:
                # 0.1.23's JSONDecodeError was a RequestException: its
                # transport handler printed "network request failed".
                err_msg = (
                    f"Replicate network request failed: {e}"
                    if legacy_mode
                    else f"Unifier replicate response is not JSON: {e}"
                )
                if legacy_mode:
                    logger.error(err_msg)
                    print(err_msg)
                    return
                raise UnifierError(err_msg) from e

            data = resp_json.get("data")
            if not data:
                err_msg = "Unifier replicate response missing 'data' field"
                if legacy_mode:
                    logger.warning(err_msg)
                    print(err_msg)
                    return
                raise UnifierError(err_msg)

            access_key_id = data.get("access_key_id")
            secret_access_key = data.get("secret_access_key")
            # Optional, server-supplied: a session token for temporary
            # (STS-style) credentials, the rclone S3 provider, and extra
            # rclone remote settings applied as given (key -> value,
            # e.g. {"force_path_style": "true"}) over the defaults below.
            session_token = data.get("session_token")
            provider = resp_json.get("provider", "Wasabi")
            rclone_config = resp_json.get("rclone_config") or {}
            data_path = resp_json.get("data_path")
            folders = resp_json.get("folders", [])
            endpoint = resp_json.get("endpoint", "https://s3.wasabisys.com")
            region = resp_json.get("region", "us-east-1")

            if not (access_key_id and secret_access_key and data_path):
                err_msg = "Unifier replicate missing required credentials or path"
                if legacy_mode:
                    logger.warning(err_msg)
                    print(err_msg)
                    return
                raise UnifierError(err_msg)

            # Handle s3:// or s3a:// prefix
            if data_path.startswith("s3://"):
                data_path = data_path[5:]
            elif data_path.startswith("s3a://"):
                data_path = data_path[6:]

            if using_rclone:
                # Prepare rclone environment
                env = os.environ.copy()
                env["RCLONE_CONFIG_UNIFIER_TYPE"] = "s3"
                env["RCLONE_CONFIG_UNIFIER_PROVIDER"] = provider
                env["RCLONE_CONFIG_UNIFIER_ENV_AUTH"] = "false"
                env["RCLONE_CONFIG_UNIFIER_ACCESS_KEY_ID"] = access_key_id
                env["RCLONE_CONFIG_UNIFIER_SECRET_ACCESS_KEY"] = secret_access_key
                env["RCLONE_CONFIG_UNIFIER_ENDPOINT"] = endpoint
                env["RCLONE_CONFIG_UNIFIER_REGION"] = region
                if session_token:
                    env["RCLONE_CONFIG_UNIFIER_SESSION_TOKEN"] = session_token
                for key, value in rclone_config.items():
                    env[f"RCLONE_CONFIG_UNIFIER_{key.upper()}"] = str(value)

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
                    name=name,
                    legacy_mode=legacy_mode,
                    session_token=session_token,
                )

        except UnifierError:
            # Already normalized above; not re-wrapped, and never
            # swallowed in the new mode.
            raise
        except requests.exceptions.RequestException as e:
            err_msg = f"Replicate network request failed: {e}"
            if legacy_mode:
                logger.error(err_msg)
                print(err_msg)
                return
            raise UnifierError(err_msg) from e
        except subprocess.CalledProcessError as e:
            err_msg = f"Rclone execution failed: {e}"
            if legacy_mode:
                logger.error(err_msg)
                print(err_msg)
                return
            raise UnifierError(err_msg) from e
        except Exception as e:
            err_msg = f"An unexpected error occurred during replication: {e}"
            if legacy_mode:
                logger.error(err_msg)
                print(err_msg)
                return
            raise UnifierError(err_msg) from e

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
        name: str,
        legacy_mode: bool = True,
        session_token: Optional[str] = None,
    ) -> None:
        """Replication using native Python (boto3)."""
        if boto3 is None:
            err_msg = "boto3 is not installed. Cannot use native Python implementation."
            if legacy_mode:
                logger.error(err_msg)
                logger.info("Please install boto3: pip install boto3")
                return
            raise UnifierError(err_msg)

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
            aws_session_token=session_token,
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
