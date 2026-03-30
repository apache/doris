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
import stat
import subprocess
import tempfile
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
HELPER = os.path.join(ROOT, "regression-test", "pipeline", "common", "stage-timer.sh")
GITHUB_HELPER = os.path.join(ROOT, "regression-test", "pipeline", "common", "github-utils.sh")


class StageTimerTest(unittest.TestCase):
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

    def _run(self, body):
        return self._run_raw('source "{}"\n{}'.format(HELPER, body))

    def test_prints_stage_summary_on_success(self):
        result = self._run(
            """
stage_timer_init "external regression"
stage_timer_enter "前置准备"
sleep 1
stage_timer_enter "启动 Doris"
stage_timer_enter "启动依赖"
stage_timer_enter "执行 Case"
stage_timer_enter "收尾归档"
"""
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("external regression 阶段耗时汇总", result.stdout)
        self.assertIn("前置准备", result.stdout)
        self.assertIn("启动 Doris", result.stdout)
        self.assertIn("启动依赖", result.stdout)
        self.assertIn("执行 Case", result.stdout)
        self.assertIn("收尾归档", result.stdout)
        self.assertIn("总耗时", result.stdout)

    def test_prints_stage_summary_on_failure(self):
        result = self._run(
            """
stage_timer_init "external regression"
stage_timer_enter "前置准备"
false
"""
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("external regression 阶段耗时汇总", result.stdout)
        self.assertIn("前置准备", result.stdout)
        self.assertIn("退出码", result.stdout)

    def test_external_pipeline_auto_hooks_print_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for name in ("deploy_cluster.sh", "run-thirdparties-docker.sh", "run-regression-test.sh"):
                script_path = os.path.join(tmpdir, name)
                with open(script_path, "w") as script:
                    script.write("#!/usr/bin/env bash\n")
                    script.write("exit 0\n")
                os.chmod(script_path, stat.S_IRWXU)

            result = self._run_raw(
                """
export teamcity_build_checkoutDir="{root}"

main() {{
    source "{github_helper}"
    echo "PREPARE"
    cd "{tmpdir}" && bash deploy_cluster.sh test_cluster
    echo "START EXTERNAL DOCKER"
    cd "{tmpdir}" && bash run-thirdparties-docker.sh --start
    echo "RUN EXTERNAL CASE"
    cd "{tmpdir}" && ./run-regression-test.sh --teamcity --clean --run
    echo "COLLECT DOCKER LOGS"
}}

main
""".format(
                    root=ROOT,
                    github_helper=GITHUB_HELPER,
                    tmpdir=tmpdir,
                )
            )

        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("external regression 阶段耗时汇总", result.stdout)
        self.assertIn("前置准备", result.stdout)
        self.assertIn("启动 Doris", result.stdout)
        self.assertIn("启动依赖", result.stdout)
        self.assertIn("执行 Case", result.stdout)
        self.assertIn("收尾归档", result.stdout)

    def test_non_external_pipeline_does_not_enable_auto_hooks(self):
        result = self._run_raw(
            """
export teamcity_build_checkoutDir="{root}"

main() {{
    source "{github_helper}"
    echo "regular pipeline"
}}

main
""".format(
                root=ROOT,
                github_helper=GITHUB_HELPER,
            )
        )

        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("external regression 阶段耗时汇总", result.stdout)


if __name__ == "__main__":
    unittest.main()
