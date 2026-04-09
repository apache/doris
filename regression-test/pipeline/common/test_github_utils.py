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

    def _run_external_collect_logs_case(self, exit_flag, need_collect_log, collect_on_success):
        with tempfile.NamedTemporaryFile("w", delete=False) as trace:
            trace_path = trace.name
        try:
            result = self._run_raw(
                """
export teamcity_build_checkoutDir="{root}"
export COLLECT_DOCKER_LOGS_ON_SUCCESS="{collect_on_success}"

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
    need_collect_log="{need_collect_log}"
    source "{github_helper}"
    collect_docker_logs docker-log-dir
}}

main
""".format(
                    root=ROOT,
                    collect_on_success=collect_on_success,
                    trace_path=trace_path,
                    exit_flag=exit_flag,
                    need_collect_log=need_collect_log,
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
            need_collect_log="false",
            collect_on_success="false",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Skip collecting docker logs on success", result.stdout)
        self.assertEqual("", trace_output)

    def test_collect_docker_logs_on_failure(self):
        result, trace_output = self._run_external_collect_logs_case(
            exit_flag="2",
            need_collect_log="false",
            collect_on_success="false",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("original:docker-log-dir", trace_output)

    def test_collect_docker_logs_when_success_collection_enabled(self):
        result, trace_output = self._run_external_collect_logs_case(
            exit_flag="0",
            need_collect_log="false",
            collect_on_success="true",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("original:docker-log-dir", trace_output)


if __name__ == "__main__":
    unittest.main()
