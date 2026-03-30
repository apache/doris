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
HELPER = os.path.join(ROOT, "regression-test", "pipeline", "common", "stage-timer.sh")


class StageTimerTest(unittest.TestCase):
    def _run(self, body):
        with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as script:
            script.write("#!/usr/bin/env bash\n")
            script.write("set -euo pipefail\n")
            script.write("export LC_ALL=C.UTF-8\n")
            script.write("source \"{}\"\n".format(HELPER))
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


if __name__ == "__main__":
    unittest.main()
