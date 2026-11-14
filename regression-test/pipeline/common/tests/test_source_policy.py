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

"""Offline contracts for source-owned CI policy; no Doris cluster or CI writes."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

PIPELINE = Path(__file__).resolve().parents[2]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


CONFIG = module('source_config', PIPELINE / 'common/source-config.py')
COMPONENTS = module('fixture_components', PIPELINE / 'external/fixture-components.py')
RESULTS = module('test_results', PIPELINE / 'common/check-test-results.py')


class SourcePolicyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copytree(PIPELINE, self.root / 'regression-test/pipeline',
                        ignore=shutil.ignore_patterns('__pycache__'))

    def prepare(self, profile):
        (self.root / 'regression-test/conf').mkdir()
        (self.root / 'regression-test/conf/regression-conf.groovy').write_text('// base\n')
        for component in ('fe', 'be', 'ms'):
            conf = self.root / 'output' / component / 'conf'
            conf.mkdir(parents=True)
            (conf / 'doris_cloud.conf').write_text('fdb_cluster=old\nbase_option=preserve\n')
        (self.root / 'output/ms/lib').mkdir()
        (self.root / 'output/ms/lib/doris_cloud').touch()
        with patch.dict(os.environ, {'DORIS_CI_PRISTINE_FRAMEWORK': 'true'}):
            CONFIG.configure(self.root, self.root / 'report', profile, sys.executable, '123',
                             self.root / 'cloud-data')

    def test_external_preserves_source_appends_without_private_exclusions(self):
        source = self.root / 'regression-test/pipeline/external/conf/regression-conf.groovy'
        source.write_text(source.read_text() + '\nexcludeSuites += ",source_appended_suite"\n')
        with patch.dict(os.environ, {'EXTERNAL_COMPONENT_EXCLUSIONS': '{"fluss":{}}'}):
            self.prepare('external')
        actual = (self.root / 'regression-test/conf/regression-conf.groovy').read_text()
        self.assertIn('excludeSuites += ",source_appended_suite"', actual)
        self.assertEqual(actual.count('excludeSuites = '), 1)
        for restored in ('test_dlf_catalog', 'test_file_cache_query_limit', 'test_hdfs_parquet_group0',
                         'test_iceberg_deletion_vector', 'test_paimon_statistics'):
            self.assertNotIn('"' + restored + '"', actual)
        self.assertIn('"test_parquet_meta_tvf"', actual)
        self.assertIn('testGroups = "external,external_docker"', actual)
        self.assertIn('System.getenv("AWS_SECRET_ACCESS_KEY")', actual)

    def test_cloud_uses_public_provider_and_custom_settings(self):
        self.prepare('cloud_p0')
        actual = (self.root / 'regression-test/conf/regression-conf-custom.groovy').read_text()
        self.assertNotIn('if (s3Endpoint ==', actual)
        self.assertIn('s3Provider = "OSS"', actual)
        for component in ('ms', 'recycler'):
            runtime = (self.root / 'output' / component / 'conf/doris_cloud.conf').read_text()
            self.assertIn('base_option=preserve', runtime)
            self.assertEqual(runtime.count('fdb_cluster'), 1)
        receipt = json.loads((self.root / 'report/config-binding.json').read_text())
        self.assertFalse(receipt['scriptExclusionsAdded'])
        self.assertFalse(receipt['new_exclusions'])

    def test_missing_or_unknown_marker_cannot_restore_legacy_policy(self):
        marker = self.root / 'regression-test/pipeline/ci-config-version'
        marker.unlink()
        with self.assertRaises(FileNotFoundError):
            CONFIG.version(self.root, 'p0')
        marker.write_text('999\n')
        with self.assertRaises(AssertionError):
            CONFIG.version(self.root, 'p0')

    def test_product_failures_restored(self):
        for profile, names in {
            'nonConcurrent': ['test_compact_seg', 'test_analyze_long_string', 'test_backup_cancelled'],
            'cloud_p0': ['scalar_agg_pushdown', 'rewrite_when_dml', 'multi_slot'],
            'p0': ['test_backup_restore', 'test_backup_restore_atomic'],
        }.items():
            filename = 'regression-conf-custom.groovy' if profile == 'cloud_p0' else 'regression-conf.groovy'
            config = (PIPELINE / profile / 'conf' / filename).read_text()
            for name in names:
                self.assertNotIn('"' + name + '"', config)

    def test_source_policy_overwrites_private_filter_preserves_concurrency(self):
        command = 'source "$1" "$2"; printf "%s|%s|%s" "$BEUT_FILTER" "$PARALLEL" "$CI_EXECUTION_TIMEOUT_MIN"'
        result = subprocess.check_output(['bash', '-eu', '-c', command, 'test',
                                          str(PIPELINE / 'common/ci-policy.sh'), 'beut'],
                                         env=dict(os.environ, BEUT_FILTER='*-ProductFailure', PARALLEL='37'), text=True)
        self.assertEqual(result, '*|37|360')

    def test_branch_policy_does_not_set_parallelism(self):
        command = 'source "$1" nonConcurrent; printf "%s|%s" "$CI_TEST_TIMEOUT_SECONDS" "$PARALLEL"'
        result = subprocess.check_output(['bash', '-eu', '-c', command, 'test',
                                          str(PIPELINE / 'common/ci-policy.sh')],
                                         env=dict(os.environ, CI_BRANCH='branch-4.1', PARALLEL='37'), text=True)
        self.assertEqual(result, '6600|37')

    def test_feut_options_follow_target_branch(self):
        command = 'source "$1" feut; printf "%s|%s" "$FEUT_TEST_JVM_OPTIONS" "$FE_UT_PARALLEL"'
        for branch, expected in [('master', '-Xmx4g -XX:TieredStopAtLevel=1'),
                                 ('branch-4.1', '-XX:TieredStopAtLevel=1')]:
            result = subprocess.check_output(['bash', '-eu', '-c', command, 'test',
                                              str(PIPELINE / 'common/ci-policy.sh')],
                                             env=dict(os.environ, CI_BRANCH=branch, FE_UT_PARALLEL='37'),
                                             text=True)
            self.assertEqual(result, expected + '|37')

    def test_components_use_public_omissions_and_ignore_private_environment(self):
        thirdparties = self.root / 'docker/thirdparties'
        thirdparties.mkdir(parents=True)
        (thirdparties / 'run-thirdparties-docker.sh').write_text(
            'DEFAULT_COMPONENTS="mysql,hive2,hudi,oceanbase"\nALL_COMPONENTS="${DEFAULT_COMPONENTS},fluss"\n')
        with patch.dict(os.environ, {'EXTERNAL_COMPONENT_EXCLUSIONS': '{"fluss":{}}'}):
            selected, receipt = COMPONENTS.select(self.root, 'a' * 40)
        self.assertEqual(selected, ['mysql', 'hive2', 'fluss'])
        self.assertEqual(set(receipt['omitted_components']), {'hudi', 'oceanbase'})
        policy = self.root / 'regression-test/pipeline/external/conf/components.json'
        policy.write_text('{"omitted_components":{"unknown":{"reason":"r","restore_when":"c"}}}')
        with self.assertRaises(ValueError):
            COMPONENTS.select(self.root, 'a' * 40)

    def test_raw_assertions_fail_even_with_mute_metadata(self):
        report = self.root / 'result.xml'
        report.write_text('<testsuite><testcase name="assertion" muted="true"><failure/></testcase></testsuite>')
        self.assertEqual(RESULTS.check([report]), 1)
        report.write_text('<testsuite><testcase name="success"/></testsuite>')
        self.assertEqual(RESULTS.check([report]), 0)
        report.write_text('<testsuite><testcase name="error"><error/></testcase></testsuite>')
        self.assertEqual(RESULTS.check([report]), 1)
        report.write_text('<testsuite><testcase name="skipped"><skipped/></testcase></testsuite>')
        with self.assertRaises(AssertionError):
            RESULTS.check([report])
        with self.assertRaises(AssertionError):
            RESULTS.check([])


if __name__ == '__main__':
    unittest.main()
