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

import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[4]
HELPER = ROOT / "bin/bootstrap_auth_fe.sh"


class BootstrapAuthTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".bootstrap-auth-test-", dir=str(ROOT))
        self.home = Path(self.temp.name)
        self.conf = self.home / "conf/fe.conf"
        self.conf.parent.mkdir()
        self.conf.write_text("# FE config\n")
        self.meta = self.home / "doris-meta"
        self.environment = dict(os.environ)
        for key in ("DORIS_ROOT_PASSWORD", "DORIS_ROOT_PASSWORD_FILE", "DORIS_ALLOW_EMPTY_PASSWORD",
                    "DB_ADMIN_USER", "DB_ADMIN_PASSWD"):
            self.environment.pop(key, None)

    def tearDown(self):
        self.temp.cleanup()

    def prepare(self):
        return subprocess.run(
            ["bash", "-euo", "pipefail", "-c",
             'source "$1"; doris_read_root_password; doris_prepare_bootstrap_auth "$2" "$3"',
             "bootstrap-auth-test", str(HELPER), str(self.conf), str(self.meta)],
            env=self.environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    def test_new_cluster_requires_explicit_credentials(self):
        result = self.prepare()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("New cluster requires", result.stderr)
        self.assertFalse(self.meta.exists())

    def test_secret_is_hashed_without_becoming_config_or_log_plaintext(self):
        password = "test-'quoted\"-password $ with spaces"
        secret = self.home / "root-password"
        secret.write_text(password + "\n")
        self.environment["DORIS_ROOT_PASSWORD_FILE"] = str(secret)
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        expected = "*" + hashlib.sha1(hashlib.sha1(password.encode("utf-8")).digest()).hexdigest().upper()
        custom = self.conf.parent / "fe_custom.conf"
        self.assertIn("initial_root_password = " + expected, custom.read_text())
        self.assertNotIn(password, custom.read_text() + self.conf.read_text() + result.stdout + result.stderr)
        self.assertEqual(0, custom.stat().st_mode & 0o077)

    def test_existing_cluster_does_not_require_or_reapply_initial_password(self):
        (self.meta / "image").mkdir(parents=True)
        (self.meta / "image/VERSION").write_text("clusterId=123\n")
        self.environment["DORIS_ROOT_PASSWORD"] = "a-different-configured-password"
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertFalse((self.conf.parent / "fe_custom.conf").exists())

    def test_effective_custom_meta_directory_is_used(self):
        self.conf.write_text("meta_dir = ${DORIS_HOME}/custom-meta\n")
        image = self.home / "custom-meta/image"
        image.mkdir(parents=True)
        (image / "VERSION").write_text("clusterId=123\n")
        self.assertEqual(0, self.prepare().returncode)

    def test_environment_paths_preserve_existing_cluster_detection(self):
        meta = self.home / "environment-meta"
        (meta / "image").mkdir(parents=True)
        (meta / "image/VERSION").write_text("clusterId=123\n")
        self.conf.write_text("meta_dir = ${BOOTSTRAP_TEST_META}\n")
        self.environment["BOOTSTRAP_TEST_META"] = str(meta)
        self.assertEqual(0, self.prepare().returncode)
        self.assertFalse((self.conf.parent / "fe_custom.conf").exists())

    def test_environment_configuration_can_explicitly_enable_test_mode(self):
        self.conf.write_text("allow_empty_password_for_root_admin = ${BOOTSTRAP_TEST_ALLOW_EMPTY}\n")
        self.environment["BOOTSTRAP_TEST_ALLOW_EMPTY"] = "TRUE"
        self.assertEqual(0, self.prepare().returncode)

    def test_secret_overrides_stale_custom_initial_hash(self):
        custom = self.conf.parent / "fe_custom.conf"
        custom.write_text("initial_root_password = stale-invalid-value\n")
        self.environment["DORIS_ROOT_PASSWORD"] = "test-password"
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(custom.read_text().splitlines()[-1].startswith("initial_root_password = *"))

    def test_test_mode_is_explicit_and_does_not_discard_a_supplied_password(self):
        self.environment["DORIS_ALLOW_EMPTY_PASSWORD"] = "true"
        self.environment["DORIS_ROOT_PASSWORD"] = "test-password"
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        config = (self.conf.parent / "fe_custom.conf").read_text()
        self.assertIn("allow_empty_password_for_root_admin = true", config)
        self.assertIn("initial_root_password = *", config)
        self.assertIn("SECURITY WARNING", result.stderr)

    def test_nonroot_management_secret_is_not_a_root_password(self):
        self.environment["DB_ADMIN_USER"] = "node_manager"
        self.environment["DB_ADMIN_PASSWD"] = "node-manager-password"
        self.assertNotEqual(0, self.prepare().returncode)

    def test_explicit_test_mode_can_be_applied_to_an_existing_member(self):
        (self.meta / "image").mkdir(parents=True)
        (self.meta / "image/VERSION").write_text("clusterId=123\n")
        self.environment["DORIS_ALLOW_EMPTY_PASSWORD"] = "true"
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        config = (self.conf.parent / "fe_custom.conf").read_text()
        self.assertIn("allow_empty_password_for_root_admin = true", config)
        self.assertNotIn("initial_root_password", config)

    def test_custom_config_directory_receives_the_effective_hash(self):
        self.conf.write_text("custom_config_dir = ${DORIS_HOME}/custom-conf\n")
        self.environment["DORIS_ROOT_PASSWORD"] = "test-password"
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("initial_root_password = *", (self.home / "custom-conf/fe_custom.conf").read_text())

    def test_explicit_fe_test_configuration_also_allows_initialization(self):
        self.conf.write_text("allow_empty_password_for_root_admin = true\n")
        self.assertEqual(0, self.prepare().returncode)

    def test_custom_config_cannot_redirect_its_own_initial_password_update(self):
        custom = self.conf.parent / "fe_custom.conf"
        custom.write_text("custom_config_dir = ${DORIS_HOME}/another-conf\n")
        self.environment["DORIS_ROOT_PASSWORD"] = "test-password"
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("initial_root_password = *", custom.read_text())
        self.assertFalse((self.home / "another-conf/fe_custom.conf").exists())

    def run_k8s_entrypoint(self, existing=False):
        # Relocate only the image's fixed installation prefix; execute the real main dispatch.
        container = self.home / "container"
        binary = container / "fe/bin"
        binary.mkdir(parents=True)
        conf = container / "fe/conf"
        conf.mkdir()
        (conf / "fe.conf").write_text("# FE config\n")
        if existing:
            image = container / "fe/doris-meta/image"
            image.mkdir(parents=True)
            (image / "VERSION").write_text("clusterId=123\n")
        shutil.copyfile(HELPER, binary / HELPER.name)
        start = binary / "start_fe.sh"
        start.write_text('#!/bin/bash\ntouch "$(dirname "$0")/started"\n')
        start.chmod(0o700)
        entrypoint = ROOT / "docker/runtime/fe/resource/init_fe.sh"
        script = entrypoint.read_text().replace('readonly DORIS_HOME="/opt/apache-doris"',
                                               'readonly DORIS_HOME="' + str(container) + '"', 1)
        self.environment["BUILD_TYPE"] = "k8s"
        result = subprocess.run(["bash", "-c", script], env=self.environment, timeout=10,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        return result, conf / "fe_custom.conf", binary / "started"

    def test_k8s_main_applies_initial_secret_before_starting_fe(self):
        secret = self.home / "k8s-root-password"
        secret.write_text("k8s-initial-password\n")
        self.environment["DORIS_ROOT_PASSWORD_FILE"] = str(secret)
        result, config, started = self.run_k8s_entrypoint()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(started.exists())
        expected = "*" + hashlib.sha1(hashlib.sha1(b"k8s-initial-password").digest()).hexdigest().upper()
        self.assertIn("initial_root_password = " + expected, config.read_text())

    def test_k8s_main_requires_credentials_before_starting_fe(self):
        result, config, started = self.run_k8s_entrypoint()
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(started.exists())
        self.assertFalse(config.exists())

    def test_k8s_main_applies_explicit_test_mode(self):
        self.environment["DORIS_ALLOW_EMPTY_PASSWORD"] = "true"
        result, config, started = self.run_k8s_entrypoint()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(started.exists())
        self.assertIn("allow_empty_password_for_root_admin = true", config.read_text())

    def test_k8s_main_preserves_existing_metadata_without_credentials(self):
        result, config, started = self.run_k8s_entrypoint(existing=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(started.exists())
        self.assertFalse(config.exists())

    def check_master(self, root_error):
        auth = self.home / "auth"
        auth.mkdir()
        (auth / "username").write_text("node_manager")
        (auth / "password").write_text("management-password")
        binary = self.home / "bin"
        binary.mkdir()
        mysql = binary / "mysql"
        mysql.write_text("#!/bin/bash\n"
                         "if [[ ${*: -1} == root ]]; then\n"
                         "  echo 'ERROR " + str(root_error) + "' >&2; exit 1\n"
                         "fi\n"
                         "[[ ${*: -1} == node_manager && $MYSQL_PWD == management-password ]]\n")
        mysql.chmod(0o700)
        self.environment.update(AUTH_PATH=str(auth), FE_QUERY_PORT="9030",
                                PATH=str(binary) + os.pathsep + self.environment["PATH"])
        return subprocess.run(["bash", str(ROOT / "docker/runtime/fe/resource/fe_check_master.sh"),
                               "127.0.0.1"], env=self.environment, timeout=10,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    def test_master_probe_uses_management_secret_after_access_denied(self):
        result = self.check_master(1045)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_master_probe_uses_management_secret_after_local_bootstrap_restriction(self):
        result = self.check_master(1820)
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
