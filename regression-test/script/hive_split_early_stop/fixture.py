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

"""Local S3 fixture and request observer for the Split early-stop regression."""

import argparse
import hashlib
import http.client
import io
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape


class Fixture:
    def __init__(self, output, files_per_partition, bucket, upstream=None):
        self.lock = threading.Lock()
        self.state = {"phase": "setup", "empty": False, "delay_ms": 0, "active": 0, "hold_listings": False}
        self.listings_ready = threading.Event()
        self.listings_ready.set()
        self.counts = {}
        self.events = (output / "s3-requests.jsonl").open("a", buffering=1)
        self.files_per_partition = files_per_partition
        self.bucket = bucket
        self.upstream = urlsplit(upstream) if upstream else None
        self.data = b""
        if not upstream:
            import pyarrow as pa
            import pyarrow.parquet as pq

            buf = io.BytesIO()
            pq.write_table(pa.table({"id": pa.array([1], type=pa.int64())}), buf,
                           compression="NONE", version="1.0")
            self.data = buf.getvalue()
        self.etag = hashlib.md5(self.data).hexdigest()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def send(self, code, body=b"", kind="application/octet-stream", headers=None):
        headers = headers or {}
        self.send_response(code)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", headers.get("content-length", str(len(body))))
        self.send_header("x-amz-bucket-region", "us-east-1")
        self.send_header("Connection", "close")
        for key, value in headers.items():
            if key != "content-length":
                self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # The query can close an outstanding connection while stopping.
                pass
        self.close_connection = True

    def do_POST(self):
        if self.path != "/__control":
            self.send(404)
            return
        patch = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if set(patch) - {"phase", "empty", "delay_ms", "hold_listings"} or patch.get("delay_ms", 0) < 0:
            self.send(400, b"Unknown control setting or negative delay")
            return
        fixture = self.server.fixture
        with fixture.lock:
            if "phase" in patch:
                fixture.counts[patch["phase"]] = {"LIST": 0, "GET": 0, "HEAD": 0}
            fixture.state.update(patch)
            if "hold_listings" in patch:
                if patch["hold_listings"]:
                    fixture.listings_ready.clear()
                else:
                    fixture.listings_ready.set()
            snapshot = dict(fixture.state)
        self.send(200, json.dumps(snapshot).encode(), "application/json")

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        fixture = self.server.fixture
        url = urlsplit(self.path)
        if url.path == "/__metrics":
            with fixture.lock:
                body = json.dumps({"state": fixture.state, "counts": fixture.counts, "fixture": {
                    "mode": "physical_proxy" if fixture.upstream else "logical_files",
                    "files_per_partition": fixture.files_per_partition,
                    "bucket": fixture.bucket, "file_bytes": len(fixture.data),
                    "upstream": fixture.upstream.geturl().rstrip("/") if fixture.upstream else None}}).encode()
            self.send(200, body, "application/json")
            return
        params = parse_qs(url.query, keep_blank_values=True)
        listing = "list-type" in params or "prefix" in params
        action = "LIST" if listing else self.command
        with fixture.lock:
            phase, empty, delay = (fixture.state[key] for key in ("phase", "empty", "delay_ms"))
            fixture.state["active"] += 1
            counter = fixture.counts.setdefault(phase, {"LIST": 0, "GET": 0, "HEAD": 0})
            counter[action] += 1
            fixture.events.write(json.dumps({"time_ms": int(time.time() * 1000),
                                             "phase": phase, "action": action,
                                             "synthetic_listing": listing and (empty or not fixture.upstream),
                                             "path": self.path}) + "\n")
        try:
            if listing:
                if not fixture.listings_ready.wait(60):
                    self.send(503, b"Test did not release the directory-listing barrier")
                    return
            if listing and (empty or not fixture.upstream):
                time.sleep(delay / 1000)
                self.list_files(params.get("prefix", [""])[0], empty)
            elif fixture.upstream:
                self.forward(delay if listing else 0)
            elif url.path.rstrip("/") == "/" + fixture.bucket:
                self.send(200)
            else:
                self.read_file()
        finally:
            with fixture.lock:
                fixture.state["active"] -= 1

    def list_files(self, prefix, empty):
        fixture = self.server.fixture
        items = []
        if not empty:
            for number in range(fixture.files_per_partition):
                key = prefix.rstrip("/") + "/file-%03d.parquet" % number
                items.append("<Contents><Key>%s</Key><LastModified>2026-10-01T00:00:00.000Z</LastModified>"
                             '<ETag>"%s"</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>'
                             % (escape(key), fixture.etag, len(fixture.data)))
        xml = ('<?xml version="1.0" encoding="UTF-8"?>'
               '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
               '<Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount>'
               '<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>%s</ListBucketResult>'
               % (escape(fixture.bucket), escape(prefix), len(items), "".join(items)))
        self.send(200, xml.encode(), "application/xml")

    def read_file(self):
        fixture = self.server.fixture
        data = fixture.data
        headers = {"etag": '"%s"' % fixture.etag,
                   "last-modified": "Thu, 01 Oct 2026 00:00:00 GMT", "accept-ranges": "bytes"}
        code = 200
        if self.headers.get("Range"):
            begin, end = self.headers["Range"].removeprefix("bytes=").split("-")
            if begin:
                begin = int(begin)
                end = min(int(end), len(data) - 1) if end else len(data) - 1
            else:
                begin, end = max(0, len(data) - int(end)), len(data) - 1
            if begin >= len(data) or end < begin:
                self.send(416, headers={"content-range": "bytes */%d" % len(data)})
                return
            headers["content-range"] = "bytes %d-%d/%d" % (begin, end, len(data))
            data = data[begin:end + 1]
            code = 206
        self.send(code, data, headers=headers)

    def forward(self, delay_ms):
        upstream = self.server.fixture.upstream
        connection_type = http.client.HTTPSConnection if upstream.scheme == "https" else http.client.HTTPConnection
        connection = connection_type(upstream.hostname, upstream.port, timeout=60)
        try:
            # Preserve Host and the original path: both participate in S3 request signing.
            connection.request(self.command, self.path, headers=dict(self.headers))
            response = connection.getresponse()
            data = response.read()
            # Delay delivery of a real listing, rather than issuing an upstream request after cancellation.
            time.sleep(delay_ms / 1000)
            headers = {key.lower(): value for key, value in response.getheaders()
                       if key.lower() in ("content-length", "etag", "last-modified", "content-range",
                                          "accept-ranges", "x-amz-request-id")}
            self.send(response.status, data, response.getheader("Content-Type"), headers)
        finally:
            connection.close()


class Server(ThreadingHTTPServer):
    # Concurrent enumerators and BE reads should not incur the default backlog's SYN retry delays.
    request_queue_size = 128
    daemon_threads = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--files-per-partition", type=int, default=100)
    parser.add_argument("--bucket", default="stream-split-test")
    parser.add_argument("--upstream", help="Observe an existing S3 endpoint instead of generating logical files")
    args = parser.parse_args()
    if not 1 <= args.files_per_partition <= 1000:
        parser.error("files-per-partition must be between 1 and 1000")
    if args.upstream:
        endpoint = urlsplit(args.upstream)
        if endpoint.scheme not in ("http", "https") or not endpoint.hostname or endpoint.path not in ("", "/"):
            parser.error("upstream must be an HTTP(S) origin without a path")
    args.output.mkdir(parents=True, exist_ok=True)
    fixture = Fixture(args.output, args.files_per_partition, args.bucket, args.upstream)
    metadata = {"mode": "physical_proxy" if args.upstream else "logical_files",
                "files_per_partition": args.files_per_partition, "bucket": args.bucket,
                "file_bytes": len(fixture.data), "port": args.port,
                "upstream": args.upstream.rstrip("/") if args.upstream else None}
    (args.output / "fixture.json").write_text(json.dumps(metadata, indent=2) + "\n")
    server = Server((args.host, args.port), Handler)
    server.fixture = fixture
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        fixture.listings_ready.set()
        fixture.events.close()


if __name__ == "__main__":
    main()
