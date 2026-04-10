#!/usr/bin/env python
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
import subprocess
import tempfile
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
GITHUB_HELPER = os.path.join(ROOT, "regression-test", "pipeline", "common", "github-utils.sh")


class GithubUtilsTest(unittest.TestCase):
    def _run_raw(self, body):
        with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as script:
            script.write("#!/usr/bin/env bash\n")
            script.write("set -euo pipefail\n")
            script.write("export LC_ALL=C.UTF-8\n")
            script.write(body)
            script_path = script.name
        try:
            return subprocess.run(
                ["bash", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                check=False,
            )
        finally:
            os.unlink(script_path)

    def _run_external_collect_logs_case(
        self,
        exit_flag,
        need_backup_log,
        collect_on_success,
        summary=None,
        failed_suites_threshold="20",
    ):
        with tempfile.NamedTemporaryFile("w", delete=False) as trace:
            trace_path = trace.name
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                if summary is not None:
                    log_dir = os.path.join(tmpdir, "output", "regression-test", "log")
                    os.makedirs(log_dir)
                    with open(os.path.join(log_dir, "doris-regression-test.fake.log"), "w") as log_file:
                        log_file.write(summary + "\n")
                result = self._run_raw(
                    """
export teamcity_build_checkoutDir="{root}"
export COLLECT_DOCKER_LOGS_ON_SUCCESS="{collect_on_success}"
export DORIS_HOME="{doris_home}"
export failed_suites_threshold="{failed_suites_threshold}"

main() {{
    if false; then
        bash deploy_cluster.sh test_cluster
        echo "START EXTERNAL DOCKER"
        echo "RUN EXTERNAL CASE"
        echo "COLLECT DOCKER LOGS"
    fi

    collect_docker_logs() {{
        echo "original:$1" >> "{trace_path}"
    }}

    exit_flag="{exit_flag}"
    need_backup_log="{need_backup_log}"
    source "{github_helper}"
    collect_docker_logs docker-log-dir
}}

main
""".format(
                        root=ROOT,
                        collect_on_success=collect_on_success,
                        doris_home=os.path.join(tmpdir, "output"),
                        failed_suites_threshold=failed_suites_threshold,
                        trace_path=trace_path,
                        exit_flag=exit_flag,
                        need_backup_log=need_backup_log,
                        github_helper=GITHUB_HELPER,
                    )
                )
            with open(trace_path) as trace_file:
                trace_output = trace_file.read()
            return result, trace_output
        finally:
            os.unlink(trace_path)

    def _run_external_shutdown_wait_case(self, exit_flag, summary=None):
        with tempfile.NamedTemporaryFile("w", delete=False) as trace:
            trace_path = trace.name
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                fake_bin = os.path.join(tmpdir, "bin")
                os.makedirs(fake_bin)
                with open(os.path.join(fake_bin, "timeout"), "w") as timeout_file:
                    timeout_file.write("#!/usr/bin/env bash\n")
                    timeout_file.write('echo "timeout:$*" >> "{}"\n'.format(trace_path))
                with open(os.path.join(fake_bin, "sleep"), "w") as sleep_file:
                    sleep_file.write("#!/usr/bin/env bash\n")
                    sleep_file.write('echo "sleep:$*" >> "{}"\n'.format(trace_path))
                os.chmod(os.path.join(fake_bin, "timeout"), 0o755)
                os.chmod(os.path.join(fake_bin, "sleep"), 0o755)

                if summary is not None:
                    log_dir = os.path.join(tmpdir, "output", "regression-test", "log")
                    os.makedirs(log_dir)
                    with open(os.path.join(log_dir, "doris-regression-test.fake.log"), "w") as log_file:
                        log_file.write(summary + "\n")

                result = self._run_raw(
                    """
export teamcity_build_checkoutDir="{root}"
export DORIS_HOME="{doris_home}"
export PATH="{fake_bin}:$PATH"

main() {{
    if false; then
        bash deploy_cluster.sh test_cluster
        echo "START EXTERNAL DOCKER"
        echo "RUN EXTERNAL CASE"
        echo "COLLECT DOCKER LOGS"
    fi
    exit_flag="{exit_flag}"
    source "{github_helper}"
    timeout -v 10m bash /tmp/fake/be/bin/stop_be.sh --grace
    sleep 300
}}

main
""".format(
                        root=ROOT,
                        doris_home=os.path.join(tmpdir, "output"),
                        fake_bin=fake_bin,
                        exit_flag=exit_flag,
                        github_helper=GITHUB_HELPER,
                    )
                )
                with open(trace_path) as trace_file:
                    trace_output = trace_file.read()
                return result, trace_output
        finally:
            os.unlink(trace_path)

    def test_skip_collect_docker_logs_on_success_by_default(self):
        result, trace_output = self._run_external_collect_logs_case(
            exit_flag="0",
            need_backup_log="false",
            collect_on_success="false",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Skip collecting docker logs on success", result.stdout)
        self.assertEqual("", trace_output)

    def test_skip_collect_docker_logs_when_summary_meets_tolerated_success_rule(self):
        result, trace_output = self._run_external_collect_logs_case(
            exit_flag="1",
            need_backup_log="false",
            collect_on_success="false",
            summary="Test 496 suites, failed 4 suites, fatal 0 scripts, skipped 0 scripts",
            failed_suites_threshold="20",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Skip collecting docker logs on tolerated success", result.stdout)
        self.assertEqual("", trace_output)

    def test_skip_collect_docker_logs_on_success_even_if_backup_requested(self):
        result, trace_output = self._run_external_collect_logs_case(
            exit_flag="0",
            need_backup_log="true",
            collect_on_success="false",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Skip collecting docker logs on success", result.stdout)
        self.assertEqual("", trace_output)

    def test_collect_docker_logs_on_failure(self):
        result, trace_output = self._run_external_collect_logs_case(
            exit_flag="2",
            need_backup_log="false",
            collect_on_success="false",
            summary="Test 496 suites, failed 40 suites, fatal 1 scripts, skipped 0 scripts",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("original:docker-log-dir", trace_output)

    def test_collect_docker_logs_when_success_collection_enabled(self):
        result, trace_output = self._run_external_collect_logs_case(
            exit_flag="0",
            need_backup_log="false",
            collect_on_success="true",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("original:docker-log-dir", trace_output)

    def test_shorten_shutdown_waits_on_tolerated_success(self):
        result, trace_output = self._run_external_shutdown_wait_case(
            exit_flag="1",
            summary="Test 496 suites, failed 4 suites, fatal 0 scripts, skipped 0 scripts",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Use shortened BE grace timeout 2m on tolerated success.", result.stdout)
        self.assertIn("Use shortened FE image wait 180s on tolerated success.", result.stdout)
        self.assertIn("timeout:-v 2m bash /tmp/fake/be/bin/stop_be.sh --grace", trace_output)
        self.assertIn("sleep:180", trace_output)

    def test_keep_shutdown_waits_on_failure(self):
        result, trace_output = self._run_external_shutdown_wait_case(
            exit_flag="2",
            summary="Test 496 suites, failed 40 suites, fatal 1 scripts, skipped 0 scripts",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("timeout:-v 10m bash /tmp/fake/be/bin/stop_be.sh --grace", trace_output)
        self.assertIn("sleep:300", trace_output)


if __name__ == "__main__":
    unittest.main()
