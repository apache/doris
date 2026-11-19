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

import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from private_rpc_tls_fixture import Fixture, PORTS, engine_environment, package_root, reserve_ports


class PrivateRpcTlsFixtureTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def package(self, parent):
        for role in PORTS:
            (parent / role / "bin").mkdir(parents=True)
            (parent / role / "bin" / ("start_" + role + ".sh")).touch()
            (parent / role / "lib").mkdir()
            (parent / role / "lib" / ("doris-fe.jar" if role == "fe" else "doris_be")).touch()
        return parent

    def test_installed_and_checkout_package_layouts(self):
        installed = self.package(self.root / "installed")
        checkout = self.root / "checkout"
        output = self.package(checkout / "output")
        self.assertEqual(installed, package_root(installed))
        self.assertEqual(output, package_root(checkout))

    def test_missing_packages_fail_instead_of_skipping(self):
        with self.assertRaisesRegex(RuntimeError, "requires local FE and BE packages"):
            package_root(self.root)

    def test_disposable_backends_keep_packaged_sanitizer_suppressions(self):
        package = self.package(self.root / "package")
        configuration = package / "be/conf"
        configuration.mkdir()
        names = ("asan_suppr.conf", "ubsan_suppr.conf", "lsan_suppr.conf")
        for name in names:
            (configuration / name).write_text("# Packaged " + name + "\n")
        fixture = Fixture(package, self.root / "logs")
        try:
            fixture.prepare()
            for cluster in ("local", "remote"):
                for name in names:
                    self.assertEqual((configuration / name).read_bytes(),
                                     (fixture.root / cluster / "be/conf" / name).read_bytes())
        finally:
            fixture.close()

    def test_port_ranges_are_distinct_and_bindable(self):
        used = set()
        first, second = reserve_ports(used), reserve_ports(used)
        self.assertTrue(set(first["fe"].values()).isdisjoint(second["fe"].values()))
        for ports in (first, second):
            offset = ports["fe"]["rpc_port"] - PORTS["fe"]["rpc_port"]
            for role, values in ports.items():
                for key, port in values.items():
                    self.assertEqual(offset, port - PORTS[role][key])
                    with socket.socket() as listener:
                        listener.bind(("127.0.0.1", port))

    def test_engines_do_not_inherit_runner_credentials(self):
        with patch.dict(os.environ, {"GH_TOKEN": "test-only", "AWS_SECRET_ACCESS_KEY": "test-only",
                                     "JAVA_HOME": "/test/jdk", "BASH_ENV": "/test/startup"}):
            environment = engine_environment()
            self.assertEqual("/test/jdk", environment["JAVA_HOME"])
            self.assertNotIn("GH_TOKEN", environment)
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
            self.assertNotIn("BASH_ENV", environment)

    def test_ports_reject_listeners_on_other_local_addresses(self):
        used = set()
        initial = reserve_ports(used)
        fallback = reserve_ports(used)
        port = initial["fe"]["rpc_port"]
        offset = port - PORTS["fe"]["rpc_port"]
        fallback_offset = fallback["fe"]["rpc_port"] - PORTS["fe"]["rpc_port"]
        with socket.socket() as listener:
            listener.bind(("127.0.0.2", port))
            with patch("private_rpc_tls_fixture.random.SystemRandom") as random_source:
                random_source.return_value.randrange.side_effect = [offset, fallback_offset]
                ports = reserve_ports(set())
        self.assertEqual(fallback, ports)

    def test_cleanup_stops_owned_process_after_partial_startup(self):
        fixture = Fixture(self.package(self.root / "package"), self.root / "logs")
        node = fixture.root / "local/fe"
        (node / "bin").mkdir(parents=True)
        environment = engine_environment()
        environment["DORIS_HOME"] = str(node)
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], env=environment)
        (node / "bin/fe.pid").write_text(str(process.pid))
        fixture.nodes = [("local", "fe", node), ("local", "be", fixture.root / "local/be")]
        try:
            fixture.close()
            self.assertIsNotNone(process.wait(timeout=5))
            self.assertFalse(fixture.root.exists())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    def test_cleanup_never_stops_a_foreign_pid(self):
        fixture = Fixture(self.package(self.root / "package"), self.root / "logs")
        node = fixture.root / "local/fe"
        (node / "bin").mkdir(parents=True)
        environment = engine_environment()
        environment["DORIS_HOME"] = str(self.root / "foreign")
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], env=environment)
        (node / "bin/fe.pid").write_text(str(process.pid))
        fixture.nodes = [("local", "fe", node)]
        try:
            fixture.close()
            self.assertIsNone(process.poll())
        finally:
            process.kill()
            process.wait(timeout=5)

    def test_log_archive_failure_still_removes_private_keys(self):
        fixture = Fixture(self.package(self.root / "package"), self.root / "logs")
        node = fixture.root / "local/fe"
        (node / "log").mkdir(parents=True)
        (fixture.root / "identity.key").write_text("test-only")
        fixture.nodes = [("local", "fe", node)]
        with patch("private_rpc_tls_fixture.shutil.copytree", side_effect=OSError("log disk full")):
            with self.assertRaisesRegex(OSError, "log disk full"):
                fixture.close()
        self.assertFalse(fixture.root.exists())


if __name__ == "__main__":
    unittest.main()
