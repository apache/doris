#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read-only Lance REST Namespace fixture for the external regression environment.

The fixture serves namespace metadata only. Doris still opens the real Lance
dataset in MinIO after DescribeTable returns its URI and storage credentials.

Additional tables can be exposed without changing this script by setting
LANCE_REST_TABLES_JSON. Keys are Lance identifiers joined with "$", for example:

    {"all_types": "s3://warehouse/lance/all_types.lance",
     "doris$items": "s3://warehouse/lance/doris/items.lance"}

Tables listed in LANCE_REST_MANAGED_TABLES_JSON are described with
managed_versioning=true and answer the ListTableVersions / DescribeTableVersion
endpoints from a static version list, the way a namespace that owns the table's
manifests would. Only the versions listed are visible, so a version that exists
in storage but is absent here behaves like one the namespace has dropped:

    {"time_travel_managed": {"uri": "s3://warehouse/lance/time_travel.lance",
                             "versions": [{"version": 1, "timestamp_millis": 1789823167597},
                                          2, 3],
                             "branches": {"dev": {"versions": [2, 3]}}}}

A version given as an object also reports its commit time, as a real namespace does; a bare
integer reports none. Doris resolves FOR TIME AS OF from the manifests' commit times either way. Tags are not served:
they live in the dataset's _refs/tags/ for managed tables too. "branches" lists the versions
recorded on each branch (manifests under <table>/tree/<branch>/_versions/), answering the
version endpoints when a request carries a branch.

Manifests are expected at their canonical V2 path,
``<table>/_versions/<u64::MAX - version>.manifest``, which is where pylance
writes them.
"""

import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse


HOST = os.environ.get("LANCE_REST_HOST", "0.0.0.0")
PORT = int(os.environ.get("LANCE_REST_PORT", "8080"))
DELIMITER = os.environ.get("LANCE_REST_DELIMITER", "$")
BEARER_TOKEN = os.environ.get(
    "LANCE_REST_BEARER_TOKEN", "doris-lance-rest-test-token"
)


def _load_tables() -> dict[tuple[str, ...], str]:
    raw_tables = os.environ.get(
        "LANCE_REST_TABLES_JSON",
        '{"all_types":"s3://warehouse/lance/all_types.lance"}',
    )
    tables = json.loads(raw_tables)
    if not isinstance(tables, dict):
        raise ValueError("LANCE_REST_TABLES_JSON must be a JSON object")

    result = {}
    for identifier, table_uri in tables.items():
        if not isinstance(identifier, str) or not isinstance(table_uri, str):
            raise ValueError("Lance table identifiers and URIs must be strings")
        parts = tuple(part for part in identifier.split(DELIMITER) if part)
        if not parts:
            raise ValueError("A Lance table identifier cannot be empty")
        result[parts] = table_uri
    return result


TABLES = _load_tables()


def _load_unprefixed_tables() -> set[tuple[str, ...]]:
    """Tables whose vended credentials use the unprefixed object-store spelling.

    A namespace may spell credentials with any alias Lance accepts, and real servers do use the
    unprefixed one, so at least one table has to exercise it.
    """
    raw = os.environ.get("LANCE_REST_UNPREFIXED_TABLES_JSON", "[]")
    identifiers = json.loads(raw)
    if not isinstance(identifiers, list):
        raise ValueError("LANCE_REST_UNPREFIXED_TABLES_JSON must be a JSON array")
    return {
        tuple(part for part in identifier.split(DELIMITER) if part)
        for identifier in identifiers
    }


UNPREFIXED_TABLES = _load_unprefixed_tables()

U64_MAX = 2**64 - 1


def _load_managed_tables() -> dict[tuple[str, ...], dict]:
    """Tables whose versions this namespace manages.

    Each entry maps to {"versions": {version: commit_millis_or_None}, "branches": {name: [versions]}}.
    A version may be given as a bare integer, in which case no commit time is reported, or as
    {"version": n, "timestamp_millis": ms}, which is what a real namespace returns.
    """
    raw = os.environ.get("LANCE_REST_MANAGED_TABLES_JSON", "{}")
    managed = json.loads(raw)
    if not isinstance(managed, dict):
        raise ValueError("LANCE_REST_MANAGED_TABLES_JSON must be a JSON object")
    result = {}
    for identifier, spec in managed.items():
        parts = tuple(part for part in identifier.split(DELIMITER) if part)
        if not parts or not isinstance(spec, dict):
            raise ValueError("A managed Lance table needs an identifier and a spec object")
        uri = spec.get("uri")
        versions = spec.get("versions")
        if not isinstance(uri, str) or not isinstance(versions, list):
            raise ValueError("A managed Lance table spec needs 'uri' and 'versions'")
        recorded: dict[int, int | None] = {}
        for entry in versions:
            if isinstance(entry, dict):
                version, timestamp = entry.get("version"), entry.get("timestamp_millis")
            else:
                version, timestamp = entry, None
            if not isinstance(version, int) or version <= 0:
                raise ValueError("Managed Lance versions must be positive integers")
            if timestamp is not None and not isinstance(timestamp, int):
                raise ValueError("Managed Lance timestamp_millis must be an integer")
            recorded[version] = timestamp
        branches = spec.get("branches", {})
        if not isinstance(branches, dict) or any(
                not isinstance(name, str) or not isinstance(b, dict) or not isinstance(b.get("versions"), list)
                or any(not isinstance(v, int) or v <= 0 for v in b["versions"])
                for name, b in branches.items()):
            raise ValueError("Managed Lance branches must map branch names to {'versions': [ints]}")
        TABLES[parts] = uri
        result[parts] = {
            "versions": dict(sorted(recorded.items())),
            "branches": {name: sorted(b["versions"]) for name, b in branches.items()},
        }
    return result


MANAGED_TABLES = _load_managed_tables()


def _object_store_path(table_uri: str) -> str:
    """The table root as Lance's object store sees it: bucket-relative for s3, no leading slash."""
    parsed = urlparse(table_uri)
    if parsed.scheme in ("s3", "s3a", "oss"):
        return parsed.path.lstrip("/")
    if parsed.scheme in ("", "file"):
        return parsed.path.lstrip("/")
    raise ValueError(f"unsupported Lance table URI scheme: {table_uri}")


def _table_version(identifier: tuple[str, ...], version: int, branch: str | None = None) -> dict:
    root = _object_store_path(TABLES[identifier])
    if branch is not None:
        # A branch is its own manifest chain under <table>/tree/<branch>/; the fixture reports
        # no commit times for branch versions.
        return {
            "version": version,
            "manifest_path": f"{root}/tree/{branch}/_versions/{U64_MAX - version}.manifest",
        }
    result = {
        "version": version,
        "manifest_path": f"{root}/_versions/{U64_MAX - version}.manifest",
    }
    timestamp = MANAGED_TABLES[identifier]["versions"][version]
    if timestamp is not None:
        result["timestamp_millis"] = timestamp
    return result


def _branch_versions(managed: dict, branch: str | None):
    """Versions recorded on the main chain (branch None) or on a branch; None if unknown."""
    if branch is None:
        return list(managed["versions"])
    return managed["branches"].get(branch)


def _storage_options(identifier: tuple[str, ...]) -> dict[str, str]:
    access_key = os.environ.get("LANCE_S3_ACCESS_KEY", "admin")
    secret_key = os.environ.get("LANCE_S3_SECRET_KEY", "password")
    region = os.environ.get("LANCE_S3_REGION", "us-east-1")
    if identifier in UNPREFIXED_TABLES:
        return {
            "access_key_id": access_key,
            "secret_access_key": secret_key,
            "region": region,
            "virtual_hosted_style_request": "false",
            # A key Doris assigns no meaning to, kept here so the pass-through stays covered.
            # The BE opens datasets with static options and never refreshes them, so this is
            # only carried, not acted on; the expiry is far enough out that it never matters.
            "expires_at_millis": os.environ.get(
                "LANCE_S3_EXPIRES_AT_MILLIS", "4102444800000"
            ),
        }
    return {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "aws_region": region,
        "aws_virtual_hosted_style_request": "false",
    }


def _decode_identifier(identifier: str) -> tuple[str, ...]:
    identifier = unquote(identifier)
    if identifier == DELIMITER:
        return ()
    return tuple(part for part in identifier.split(DELIMITER) if part)


def _namespace_exists(namespace: tuple[str, ...]) -> bool:
    if not namespace:
        return True
    return any(
        len(identifier) > len(namespace)
        and identifier[: len(namespace)] == namespace
        for identifier in TABLES
    )


class LanceRestHandler(BaseHTTPRequestHandler):
    server_version = "DorisLanceRestFixture/1.0"

    def do_GET(self) -> None:
        path = urlparse(self.path).path.rstrip("/")
        if path == "/health":
            self._write_json(200, {"status": "ok"})
            return
        if not self._authorized():
            return

        table_list_match = re.fullmatch(r"/v1/namespace/(.+)/table/list", path)
        if table_list_match:
            parent = _decode_identifier(table_list_match.group(1))
            tables = sorted(
                identifier[-1]
                for identifier in TABLES
                if len(identifier) == len(parent) + 1
                and identifier[: len(parent)] == parent
            )
            self._write_json(200, {"tables": tables})
            return

        namespace_list_match = re.fullmatch(r"/v1/namespace/(.+)/list", path)
        if namespace_list_match:
            parent = _decode_identifier(namespace_list_match.group(1))
            namespaces = sorted(
                {
                    identifier[len(parent)]
                    for identifier in TABLES
                    if len(identifier) > len(parent) + 1
                    and identifier[: len(parent)] == parent
                }
            )
            self._write_json(200, {"namespaces": namespaces})
            return

        self._write_json(404, {"error": "not found", "code": 4})

    def do_POST(self) -> None:
        path = urlparse(self.path).path.rstrip("/")
        if not self._authorized():
            return
        raw_body = self._read_body()
        try:
            body = json.loads(raw_body) if raw_body else {}
        except ValueError:
            self._write_json(400, {"error": "request body is not JSON", "code": 13})
            return

        # Version endpoints first: the generic describe pattern would otherwise swallow
        # "<id>/version/describe" as a table id.
        version_list_match = re.fullmatch(r"/v1/table/(.+)/version/list", path)
        if version_list_match:
            identifier = _decode_identifier(version_list_match.group(1))
            managed = MANAGED_TABLES.get(identifier)
            if managed is None:
                if identifier not in TABLES:
                    self._write_json(404, {"error": "table not found", "code": 4})
                else:
                    # 13 = InvalidInput: the table exists but its versions are not managed here.
                    self._write_json(400, {"error": "table versions are not managed", "code": 13})
                return
            query = parse_qs(urlparse(self.path).query)
            branch = query.get("branch", [None])[0]
            versions = _branch_versions(managed, branch)
            if versions is None:
                # 22 = TableBranchNotFound
                self._write_json(404, {"error": f"table branch {branch} not found", "code": 22})
                return
            ordered = list(versions)
            if query.get("descending", ["false"])[0].lower() == "true":
                ordered.reverse()
            limit = query.get("limit", [None])[0]
            if limit is not None:
                if not limit.isdigit():
                    self._write_json(400, {"error": "limit must be a non-negative integer", "code": 13})
                    return
                ordered = ordered[: int(limit)]
            self._write_json(
                200,
                {"versions": [_table_version(identifier, v, branch) for v in ordered]},
            )
            return

        version_describe_match = re.fullmatch(r"/v1/table/(.+)/version/describe", path)
        if version_describe_match:
            identifier = _decode_identifier(version_describe_match.group(1))
            managed = MANAGED_TABLES.get(identifier)
            if managed is None:
                if identifier not in TABLES:
                    self._write_json(404, {"error": "table not found", "code": 4})
                else:
                    self._write_json(400, {"error": "table versions are not managed", "code": 13})
                return
            branch = body.get("branch") if isinstance(body, dict) else None
            versions = _branch_versions(managed, branch)
            if versions is None:
                self._write_json(404, {"error": f"table branch {branch} not found", "code": 22})
                return
            version = body.get("version") if isinstance(body, dict) else None
            if not isinstance(version, int) or version not in versions:
                self._write_json(
                    404, {"error": f"table version {version} not found", "code": 11}
                )
                return
            self._write_json(200, {"version": _table_version(identifier, version, branch)})
            return

        describe_match = re.fullmatch(r"/v1/table/(.+)/describe", path)
        if describe_match:
            identifier = _decode_identifier(describe_match.group(1))
            table_uri = TABLES.get(identifier)
            if table_uri is None:
                self._write_json(404, {"error": "table not found", "code": 4})
                return
            self._write_json(
                200,
                {
                    "table": identifier[-1],
                    "namespace": list(identifier[:-1]),
                    "location": table_uri,
                    "table_uri": table_uri,
                    "storage_options": _storage_options(identifier),
                    "managed_versioning": identifier in MANAGED_TABLES,
                    "is_only_declared": False,
                },
            )
            return

        table_exists_match = re.fullmatch(r"/v1/table/(.+)/exists", path)
        if table_exists_match:
            identifier = _decode_identifier(table_exists_match.group(1))
            if identifier in TABLES:
                self._write_empty(200)
                return
            if not _namespace_exists(identifier[:-1]):
                self._write_json(404, {"error": "namespace not found", "code": 1})
                return
            self._write_json(404, {"error": "table not found", "code": 4})
            return

        self._write_json(404, {"error": "not found", "code": 4})

    def _authorized(self) -> bool:
        if self.headers.get("Authorization") == f"Bearer {BEARER_TOKEN}":
            return True
        self._read_body()
        self._write_json(401, {"error": "unauthorized", "code": 16})
        return False

    def _read_body(self) -> bytes:
        content_length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(content_length) if content_length else b""

    def _write_json(self, status: int, body: dict) -> None:
        response = json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def _write_empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, message: str, *args: object) -> None:
        print(f"{self.address_string()} - {message % args}", flush=True)


if __name__ == "__main__":
    print(
        f"Starting Lance REST Namespace fixture on {HOST}:{PORT} "
        f"with {len(TABLES)} table(s), {len(MANAGED_TABLES)} managed",
        flush=True,
    )
    ThreadingHTTPServer((HOST, PORT), LanceRestHandler).serve_forever()
