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
"""Regression tests for source-scoped GCS dependency exceptions.

Run with: python3 -m unittest discover -s tools/be-java-plugins -p 'test_*.py'
"""
import os
import subprocess
import tempfile
import unittest
from unittest import mock
import zipfile

import check_plugin_layout as layout


class GcsClosureTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.doris_jar = self.jar("scanner.jar", ["org/apache/doris/Scanner.class"])
        self.gcs_jar = self.jar("gcs.jar", [
            "com/google/cloud/hadoop/fs/gcs/GoogleHadoopFileSystem.class"])
        self.spi_jar = self.jar("spi.jar", [])

    def jar(self, name, classes):
        path = os.path.join(self.directory.name, name)
        with zipfile.ZipFile(path, "w") as jar:
            for entry in classes:
                jar.writestr(entry, b"fixture")
        return path

    def check_edges(self, plugin, edges):
        output = "\n".join("   %s -> %s not found" % edge for edge in edges)
        failures = []
        with mock.patch.object(layout.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, stdout=output, stderr="")) as run:
            layout.check_closure_self_contained(
                plugin, [self.doris_jar, self.gcs_jar], self.spi_jar,
                lambda check, message: failures.append((check, message)))
        # The GCS jar remains a jdeps root; the fix must not hide the whole jar.
        self.assertIn(self.gcs_jar, run.call_args.args[0])
        return failures

    def test_optional_gcs_edges(self):
        edges = [
            (layout._GCS + "com.google.api.client.extensions.appengine.http.UrlFetchRequest",
             "com.google.appengine.api.urlfetch.HTTPRequest"),
            (layout._GCS_NETTY + "handler.codec.compression.BrotliDecoder",
             "com.aayushatharva.brotli4j.decoder.DecoderJNI"),
            (layout._GCS_NETTY + "handler.ssl.BouncyCastlePemReader",
             "org.bouncycastle.openssl.PEMParser"),
            (layout._GCS + "io.opentelemetry.sdk.trace.ExtendedSdkTracer",
             layout._GCS + "io.opentelemetry.api.incubator.trace.ExtendedTracer"),
            (layout._GCS + "io.grpc.testing.GrpcCleanupRule", "org.junit.runners.model.Statement"),
            (layout._GCS + "io.grpc.testing.GrpcServerRule", "org.junit.rules.ExternalResource"),
            ("com.google.cloud.hadoop.fs.gcs.auth.GcsDtFetcher",
             "com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem"),
        ]
        for plugin in ("hudi", "iceberg", "paimon"):
            with self.subTest(plugin=plugin):
                self.assertEqual([], self.check_edges(plugin, edges))
                with mock.patch.object(layout, "GCS_CLOSURE_ALLOWLIST", []):
                    self.assertEqual(len(edges), len(self.check_edges(plugin, edges)))

    def test_same_target_from_runtime_code_still_fails(self):
        target = "org.bouncycastle.openssl.PEMParser"
        edges = [
            (layout._GCS_NETTY + "handler.ssl.BouncyCastlePemReader", target),
            ("org.apache.doris.GcsReader", target),
        ]
        for plugin in ("hudi", "iceberg", "paimon"):
            failures = self.check_edges(plugin, edges)
            self.assertEqual(1, len(failures))
            self.assertIn("org.apache.doris.GcsReader", failures[0][1])
            self.assertIn("Either ship the jar", failures[0][1])

    def test_required_gcs_dependencies_still_fail(self):
        edges = [
            ("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
             "org.apache.hadoop.fs.FileSystem"),
            (layout._GCS + "com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl",
             layout._GCS + "com.google.auth.oauth2.GoogleCredentials"),
            (layout._GCS + "com.google.auth.oauth2.ServiceAccountCredentials",
             "org.bouncycastle.openssl.PEMParser"),
            (layout._GCS_NETTY + "handler.ssl.JdkSslContext", "missing.RequiredTlsClass"),
        ]
        for plugin in ("hudi", "iceberg", "paimon"):
            self.assertEqual(len(edges), len(self.check_edges(plugin, edges)))

    def test_exceptions_do_not_apply_to_other_plugins(self):
        edges = [(layout._GCS + "io.grpc.testing.GrpcCleanupRule", "org.junit.rules.ExternalResource")]
        self.assertEqual(1, len(self.check_edges("jdbc", edges)))

    def test_jdeps_failure_is_not_ignored(self):
        failures = []
        with mock.patch.object(layout.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 1, stdout="", stderr="broken classpath")):
            layout.check_closure_self_contained(
                "paimon", [self.doris_jar, self.gcs_jar], self.spi_jar,
                lambda check, message: failures.append(message))
        self.assertEqual(1, len(failures))
        self.assertIn("jdeps exited 1", failures[0])


if __name__ == "__main__":
    unittest.main()
