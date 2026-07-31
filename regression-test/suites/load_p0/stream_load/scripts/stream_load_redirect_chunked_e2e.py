#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Exercise the FE redirect path with a chunked stream load request."""

import argparse
import base64
import http.client
import json
import sys
import time
import uuid


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send a chunked stream load request to FE and capture the redirect result."
    )
    parser.add_argument("--host", required=True, help="FE host")
    parser.add_argument("--fe-http-port", required=True, type=int, help="FE HTTP port")
    parser.add_argument("--user", required=True, help="FE HTTP user")
    parser.add_argument("--password", default="", help="FE HTTP password")
    parser.add_argument("--db", required=True, help="Target database")
    parser.add_argument("--table", required=True, help="Target table")
    parser.add_argument("--payload-mb", type=int, default=8, help="Approximate payload size in MiB")
    parser.add_argument("--chunk-kb", type=int, default=8, help="Chunk size in KiB")
    parser.add_argument("--sleep-ms", type=int, default=10, help="Delay between chunks in milliseconds")
    parser.add_argument("--connect-timeout", type=int, default=5, help="Connect timeout in seconds")
    parser.add_argument("--read-timeout", type=int, default=120, help="Read timeout in seconds")
    return parser.parse_args()


def build_auth_header(user, password):
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def build_csv_chunk(chunk_bytes, chunk_index):
    # Generate deterministic CSV rows so the request body is valid for stream load.
    rows = []
    current = 0
    row_index = chunk_index * 100000
    payload_width = max(8, min(256, max(1, chunk_bytes // 8)))
    while True:
        row = f"{row_index},payload_{chunk_index}_{'x' * payload_width}\n"
        row_size = len(row.encode("utf-8"))
        if rows and current + row_size > chunk_bytes:
            break
        rows.append(row)
        current += row_size
        row_index += 1
        if current >= chunk_bytes:
            break
    return "".join(rows).encode("utf-8")


def chunked_csv_generator(total_bytes, chunk_bytes, sleep_ms):
    sent = 0
    chunk_index = 0
    while sent < total_bytes:
        current_size = min(chunk_bytes, total_bytes - sent)
        yield build_csv_chunk(current_size, chunk_index)
        sent += current_size
        chunk_index += 1
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)


def main():
    args = parse_args()
    path = f"/api/{args.db}/{args.table}/_stream_load"
    conn = http.client.HTTPConnection(args.host, args.fe_http_port, timeout=args.read_timeout)
    label = f"stream_load_redirect_regression_{uuid.uuid4().hex[:12]}"
    total_bytes = args.payload_mb * 1024 * 1024
    chunk_bytes = args.chunk_kb * 1024
    started = time.time()

    try:
        # Send the request body with explicit chunk framing so the client keeps writing
        # while FE returns the redirect response.
        conn.putrequest("PUT", path)
        conn.putheader("Authorization", build_auth_header(args.user, args.password))
        conn.putheader("Expect", "100-continue")
        conn.putheader("Transfer-Encoding", "chunked")
        conn.putheader("column_separator", ",")
        conn.putheader("label", label)
        conn.endheaders()

        for chunk in chunked_csv_generator(total_bytes, chunk_bytes, args.sleep_ms):
            conn.send(f"{len(chunk):X}\r\n".encode("ascii"))
            conn.send(chunk)
            conn.send(b"\r\n")
        conn.send(b"0\r\n\r\n")

        response = conn.getresponse()
        body = response.read().decode("utf-8", errors="replace")
        result = {
            "status_code": response.status,
            "elapsed_seconds": round(time.time() - started, 3),
            "headers": dict(response.getheaders()),
            "body": body[:2000],
            "exception_type": None,
            "exception": None,
            "label": label,
        }
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:
        result = {
            "status_code": None,
            "elapsed_seconds": round(time.time() - started, 3),
            "headers": {},
            "body": "",
            "exception_type": type(exc).__name__,
            "exception": repr(exc),
            "label": label,
        }
        print(json.dumps(result, ensure_ascii=False))
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
