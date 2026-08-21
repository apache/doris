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
"""

import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse


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
        self._read_body()

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
                    "managed_versioning": False,
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
        f"with {len(TABLES)} table(s)",
        flush=True,
    )
    ThreadingHTTPServer((HOST, PORT), LanceRestHandler).serve_forever()
