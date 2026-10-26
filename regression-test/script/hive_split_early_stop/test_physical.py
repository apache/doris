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

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from materialize import verify
from prepare import fixture_metadata
from report import physical_evidence


class PhysicalFixtureTest(unittest.TestCase):
    def objects(self):
        return [{"Key": "data/p=%d/file-%03d.parquet" % (p, n), "Size": 4, "ETag": '"abcd"'}
                for p in range(2) for n in range(2)]

    def verify_objects(self, objects):
        client = Mock()
        client.get_paginator.return_value.paginate.return_value = [{"Contents": objects}]
        marker = {"partitions": 2, "files_per_partition": 2, "file_bytes": 4, "file_md5": "abcd"}
        return verify(client, "test", marker)

    def test_complete_physical_shape_is_verified(self):
        self.assertEqual(4, self.verify_objects(self.objects()))

    def test_missing_physical_file_is_not_a_verified_dataset(self):
        with self.assertRaisesRegex(ValueError, "counts do not match"):
            self.verify_objects(self.objects()[:-1])

    def test_noncanonical_prefix_cannot_substitute_for_a_real_partition(self):
        objects = self.objects()
        objects[0]["Key"] = "data/p=00/file-000.parquet"
        with self.assertRaisesRegex(ValueError, "Noncanonical"):
            self.verify_objects(objects)

    def test_corrupt_files_are_not_counted_as_valid_physical_objects(self):
        for field, value in (("Size", 5), ("ETag", '"wrong"')):
            with self.subTest(field=field):
                objects = self.objects()
                objects[0][field] = value
                with self.assertRaisesRegex(ValueError, "size or checksum"):
                    self.verify_objects(objects)

    def test_an_unexpected_object_invalidates_the_dataset_shape(self):
        objects = self.objects()
        objects.append({"Key": "data/p=2/file-000.parquet", "Size": 4, "ETag": '"abcd"'})
        with self.assertRaisesRegex(ValueError, "outside the requested"):
            self.verify_objects(objects)

    def test_prepare_requires_a_verified_manifest_matching_the_actual_proxy(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[3] / "output") as directory:
            path = Path(directory) / "manifest.json"
            args = SimpleNamespace(physical_manifest=path, partitions=2, files_per_partition=2, bucket="test")
            proxy = {"mode": "physical_proxy", "files_per_partition": 2, "bucket": "test",
                     "upstream": "http://127.0.0.1:12345"}
            manifest = {"status": "VERIFIED", "mode": "physical_files", "verified_objects": 4,
                        "partitions": 2, "files_per_partition": 2, "bucket": "test", "rows_per_file": 1,
                        "prefix": "data/", "endpoint": proxy["upstream"], "file_bytes": 4}
            path.write_text(json.dumps(manifest))
            self.assertEqual(4, fixture_metadata(args, proxy)["physical_files"])
            for key, value in (("status", "PENDING"), ("verified_objects", 3),
                               ("endpoint", "http://127.0.0.1:23456"), ("rows_per_file", 2)):
                with self.subTest(key=key):
                    path.write_text(json.dumps(dict(manifest, **{key: value})))
                    with self.assertRaisesRegex(ValueError, "manifest does not match"):
                        fixture_metadata(args, proxy)

    def test_a_physical_proxy_cannot_be_reported_as_a_logical_fixture(self):
        args = SimpleNamespace(physical_manifest=None, partitions=2, files_per_partition=2, bucket="test")
        with self.assertRaisesRegex(ValueError, "requires --physical-manifest"):
            fixture_metadata(args, {"mode": "physical_proxy", "bucket": "test", "files_per_partition": 2})

    def test_report_requires_physical_evidence_for_the_tested_dataset(self):
        fixture = {"mode": "physical_proxy", "bucket": "test", "upstream": "http://localhost:12345"}
        hms = {"partitions": 2, "files_per_partition": 2, "file_bytes": 4, "physical_files": 4}
        manifest = dict(hms, status="VERIFIED", mode="physical_files", verified_objects=4,
                        bucket="test", endpoint=fixture["upstream"])
        self.assertEqual(manifest, physical_evidence(fixture, hms, manifest))
        for key, value in (("status", "PENDING"), ("partitions", 3), ("verified_objects", 3),
                           ("endpoint", "http://localhost:23456")):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    physical_evidence(fixture, hms, dict(manifest, **{key: value}))


if __name__ == "__main__":
    unittest.main()
