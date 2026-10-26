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

import http.client
import json
import tempfile
import threading
import time
import unittest
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fixture import Fixture, Handler, Server


class FixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[3] / "output")
        cls.fixture = Fixture(Path(cls.directory.name), 100, "test-bucket")
        cls.server = Server(("127.0.0.1", 0), Handler)
        cls.server.fixture = cls.fixture
        cls.worker = threading.Thread(target=cls.server.serve_forever)
        cls.worker.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.worker.join(5)
        cls.fixture.events.close()
        cls.directory.cleanup()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=5)
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def control(self, **settings):
        return self.request("POST", "/__control", json.dumps(settings), {"Content-Type": "application/json"})

    def setUp(self):
        self.control(phase=self.id(), empty=False, delay_ms=0, hold_listings=False)

    def test_listing_is_readable_parquet_not_just_file_names(self):
        code, _, body = self.request("GET", "/test-bucket?list-type=2&prefix=data/p=0/")
        self.assertEqual(200, code)
        contents = ET.fromstring(body).findall("{*}Contents")
        self.assertEqual(100, len(contents))
        key = contents[0].find("{*}Key").text
        code, headers, body = self.request("GET", "/test-bucket/" + key)
        self.assertEqual(200, code)
        self.assertEqual(int(contents[0].find("{*}Size").text), len(body))
        self.assertEqual(b"PAR1", body[:4])
        self.assertEqual(b"PAR1", body[-4:])
        self.assertEqual(str(len(body)), headers["Content-Length"])

    def test_head_and_range_reads_use_the_complete_file_size(self):
        code, headers, body = self.request("HEAD", "/test-bucket/file.parquet")
        self.assertEqual((200, b""), (code, body))
        self.assertEqual(str(len(self.fixture.data)), headers["Content-Length"])
        code, headers, body = self.request("GET", "/test-bucket/file.parquet", headers={"Range": "bytes=-4"})
        self.assertEqual((206, b"PAR1"), (code, body))
        self.assertEqual("bytes %d-%d/%d" % (len(self.fixture.data) - 4, len(self.fixture.data) - 1,
                                           len(self.fixture.data)), headers["content-range"])

    def test_invalid_range_fails(self):
        code, _, _ = self.request("GET", "/test-bucket/file.parquet", headers={"Range": "bytes=999999-"})
        self.assertEqual(416, code)

    def test_control_cannot_forge_the_active_request_count(self):
        self.assertEqual(400, self.control(active=0)[0])

    def test_listing_barrier_can_be_released_without_an_enumeration_worker(self):
        self.control(hold_listings=True)
        with ThreadPoolExecutor(max_workers=1) as executor:
            listing = executor.submit(self.request, "GET", "/test-bucket?prefix=data/")
            try:
                deadline = time.monotonic() + 5
                while self.fixture.state["active"] != 1 and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertEqual(1, self.fixture.state["active"])
                self.assertFalse(listing.done())
                self.control(hold_listings=False)
                self.assertEqual(200, listing.result(5)[0])
            finally:
                self.control(hold_listings=False)


if __name__ == "__main__":
    unittest.main()
