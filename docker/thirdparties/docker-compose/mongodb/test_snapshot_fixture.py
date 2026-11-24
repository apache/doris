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

"""Check the External fixture network and cleanup contracts."""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from snapshot_fixture import Fixture


class FixtureNetworkTest(unittest.TestCase):
    def test_failed_worker_records_only_its_component_and_exit_status(self):
        launcher = Path(__file__).resolve().parents[2] / "run-thirdparties-docker.sh"
        source = launcher.read_text()
        functions = re.search(r"(?ms)^declare -A START_PIDS=.*?(?=^start_es\(\))", source).group(0)
        marker = re.search(r"(?ms)^record_start_failure\(\) \{.*?^\}", source)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = "LOG_ROOT=" + shlex.quote(directory) + "\n" + """
START_CLEANUP_ON_FAILURE=0
START_PROGRESS_INTERVAL=60
docker_cli() { :; }
""" + (marker.group(0) if marker else "") + "\n" + functions + """
worker() { printf 'private-worker-output\\n'; return 23; }
launch_component fixture "$LOG_ROOT/start_fixture.log" worker
wait_for_started_jobs
"""
            result = subprocess.run(["bash", "-ec", script], capture_output=True)
            self.assertEqual(1, result.returncode)
            evidence = root / "start_fixture_exit_23.log"
            self.assertTrue(evidence.exists(), "The failed component/status must survive filtered CI diagnostics")
            self.assertEqual("Fixture exited before readiness:\n", evidence.read_text())

    def test_launcher_failure_keeps_its_line_and_original_exit_status(self):
        launcher = Path(__file__).resolve().parents[2] / "run-thirdparties-docker.sh"
        header = re.search(r"(?ms)^LOG_ROOT=.*?(?=^JUICEFS_LOCAL_BIN=)", launcher.read_text()).group(0)
        for command, status, has_line in (("exit 0", 0, False), ("(exit 17) & wait $! || true", 0, False),
                                          ("exit 17", 17, False),
                                          ("(exit 17)", 17, True)):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                script = "ROOT=" + shlex.quote(directory) + "\n" + header
                exit_line = len(script.splitlines()) + 1
                script += command + "\n"
                result = subprocess.run(["bash", "-ec", script], capture_output=True)
                self.assertEqual(status, result.returncode)
                evidence = list((root / "logs").glob("start_launcher_*.log"))
                if status:
                    self.assertEqual(2 if has_line else 1, len(evidence))
                    self.assertIn("start_launcher_exit_17.log", [item.name for item in evidence])
                    if has_line:
                        self.assertIn("start_launcher_line_" + str(exit_line) + "_exit_17.log",
                                      [item.name for item in evidence])
                    for item in evidence:
                        self.assertEqual("Fixture exited before readiness:\n", item.read_text())
                else:
                    self.assertEqual([], evidence)

    def test_launcher_shares_the_network_for_loopback_replica_members(self):
        launcher = Path(__file__).resolve().parents[2] / "run-thirdparties-docker.sh"
        function = re.search(r"(?ms)^start_mongodb\(\) \{.*?^\}", launcher.read_text()).group(0)
        for host_os, advertised_host, expected_network in (
                ("Linux", "127.0.0.1", "host"), ("Linux", "localhost", "host"),
                ("Linux", "fixture.example", "bridge"), ("Darwin", "127.0.0.1", "host")):
            with self.subTest(host_os=host_os, host=advertised_host), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                mongodb = root / "docker-compose/mongodb"
                python = mongodb / ".ci-venv/bin/python"
                python.parent.mkdir(parents=True)
                python.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$MONGO_FIXTURE_TEST_TRACE\"\n")
                python.chmod(0o700)
                (mongodb / "mongodb.env.tpl").write_text("")
                environment = os.environ.copy()
                environment["MONGO_FIXTURE_TEST_TRACE"] = str(root / "called")
                script = ("ROOT=" + shlex.quote(str(root)) + "\nHOST_OS=" + host_os
                          + "\nIP_HOST=" + shlex.quote(advertised_host) + "\n") + """
STOP=0
envsubst() { cat; }
start_rendered_compose_stack() { :; }
require_rootless_host_network() { :; }
""" + function + "\nstart_mongodb\n"
                subprocess.run(["bash", "-ec", script], env=environment, check=True, capture_output=True)
                arguments = (root / "called").read_text().splitlines()
                self.assertEqual("ci_fixture.py", Path(arguments[0]).name)
                self.assertEqual(advertised_host, arguments[arguments.index("--advertised-host") + 1])
                self.assertEqual(expected_network, arguments[arguments.index("--network-mode") + 1],
                                 "Replica peers must share the advertised loopback network")

    def test_bridge_publishes_the_advertised_port_and_uses_the_namespace_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(state_dir=directory, docker_command="docker", advertised_host="127.0.0.1",
                                      image="mongo:8.0.20", network_mode="bridge", container_user="0:0")
            fixture = Fixture(args)
            fixture.state = {"prefix": "fixture", "containers": []}
            commands = []
            fixture.docker = lambda *arguments: commands.append(arguments)
            fixture.save = lambda: None
            fixture.start("member", 28016, ["--replSet", "fixture_rs"])
            command = commands[0]
            self.assertIn("--publish", command)
            self.assertEqual("28016:28016", command[command.index("--publish") + 1])
            self.assertEqual("bridge", command[command.index("--network") + 1])
            self.assertEqual("0.0.0.0", command[command.index("--bind_ip") + 1])
            self.assertEqual("0:0", command[command.index("--user") + 1])
            self.assertIn("/data/configdb:rw,size=64m,mode=1777", command)

    def test_cleanup_continues_past_a_reserved_container_that_never_started(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(state_dir=directory, docker_command="docker")
            fixture = Fixture(args)
            fixture.state = {"containers": ["owned-first", "owned-not-started", "owned-last"]}

            def docker(*arguments):
                if arguments[0] == "rm":
                    raise subprocess.CalledProcessError(1, "docker rm")
                return "unrelated-container"

            fixture.docker = docker
            with patch("snapshot_fixture.subprocess.run") as remove:
                fixture.remove()
                self.assertEqual([], fixture.state["containers"])
                self.assertEqual(["docker", "rm", "-f", "-v", "owned-first", "owned-not-started", "owned-last"],
                                 remove.call_args.args[0])

    def test_launcher_cleanup_does_not_depend_on_worker_local_metadata(self):
        launcher = Path(__file__).resolve().parents[2] / "run-thirdparties-docker.sh"
        function = re.search(r"(?ms)^cleanup_started_stacks\(\) \{.*?^\}", launcher.read_text()).group(0)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python = root / "docker-compose/mongodb/.ci-venv/bin/python"
            python.parent.mkdir(parents=True)
            python.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$MONGO_FIXTURE_TEST_TRACE\"\n")
            python.chmod(0o700)
            environment = os.environ.copy()
            environment["MONGO_FIXTURE_TEST_TRACE"] = str(root / "called")
            environment["MONGODB_CI_STATE_DIR"] = str(root / "state")
            script = "ROOT=" + shlex.quote(str(root)) + "\n" + """
START_CLEANUP_ON_FAILURE=1
START_ORDER=(mongodb)
declare -A START_COMPOSE_FILES=()
declare -A START_ENV_FILES=()
compose_down_stack() { :; }
""" + function + "\ncleanup_started_stacks\n"
            subprocess.run(["bash", "-c", script], env=environment, check=True, capture_output=True)
            self.assertTrue((root / "called").exists(), "Worker metadata must not hide MongoDB cleanup")
            self.assertIn("--remove", (root / "called").read_text())


if __name__ == "__main__":
    unittest.main()
