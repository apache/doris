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

"""Contract tests for comment retries, revision binding, and artifact reuse."""
import copy
import importlib.util
import json
import os
import subprocess
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('current', Path(__file__).parents[1] / 'teamcity-current.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

PR = {'number': 123, 'state': 'open', 'draft': False,
      'head': {'sha': 'a' * 40}, 'base': {'ref': 'master'}}
EVENT = {'comment': {'id': 45, 'body': 'run cloudut', 'updated_at': '2026-11-11T00:00:00Z',
                     'user': {'login': 'committer', 'id': 1}}}


def build(bid=1, state='running', key=None):
    result = {'id': bid, 'number': str(bid), 'state': state, 'status': 'SUCCESS',
              'buildTypeId': 'Doris_Compile', 'branchName': 'pull/123',
              'revisions': {'revision': [{'version': 'a' * 40}]},
              'properties': {'property': []}}
    if key:
        result['properties']['property'].append({'name': 'env.CI_COMMENT_REQUEST_KEY', 'value': key})
    return result


def manifest():
    return {'source': {'repository': 'apache/doris', 'pr_number': 123, 'pr_head_sha': 'a' * 40,
                       'target_branch': 'master', 'target_head_sha': 'b' * 40,
                       'checkout_sha': 'c' * 40, 'modified': False},
            'producer': {'compiled_in_build': 1, 'packaged_in_build': 1, 'image': 'image'},
            'trust_class': 'pr-merge', 'expires_at': '2999-01-01T00:00:00Z',
            'frameworkSource': {'modified': False}, 'tools': {'jdk': '17'},
            'unchangedTestTree': {'treeSha256': 'd' * 64},
            'packages': {name: {'sha256': 'e' * 64, 'bytes': 123}
                         for name in ['binary.tar.gz', 'cases.tar.gz']}}


class TriggerTests(unittest.TestCase):
    def setUp(self):
        self.github = Mock()
        self.github.pr.return_value = copy.deepcopy(PR)
        self.tc = Mock(web_base='https://ci.example')
        self.tc.builds.return_value = []
        self.tc.enqueue.side_effect = lambda typ, pr, params, key, deps: build(10, 'queued', key)
        self.trigger = m.Trigger(self.github, self.tc, EVENT, PR)

    def test_legacy_request_does_not_trace_credentials(self):
        helper = Path(__file__).parents[1] / 'teamcity-utils.sh'
        command = 'source "$1"; curl() { return 0; }; teamcity_request --fail https://ci.example'
        environment = {**os.environ, 'TEAMCITY_LEGACY_USER': 'test-user-sentinel',
                       'TEAMCITY_LEGACY_PASSWORD': 'test-password-sentinel'}
        result = subprocess.run(['bash', '-x', '-c', command, 'probe', str(helper)],
                                env=environment, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(environment['TEAMCITY_LEGACY_USER'], result.stdout + result.stderr)
        self.assertNotIn(environment['TEAMCITY_LEGACY_PASSWORD'], result.stdout + result.stderr)
        environment['TEAMCITY_LEGACY_PASSWORD'] = ''
        result = subprocess.run(['bash', '-c', command, 'probe', str(helper)],
                                env=environment, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def test_commands(self):
        self.assertEqual(m.parse_command('please run nonConcurrent 3'), ('run', 'nonConcurrent', 3))
        for command in ['run p0 0', 'run p0 21', 'run p0; run external', 'rerun p0', 'skip p0']:
            with self.subTest(command=command), self.assertRaises(m.Error):
                m.parse_command(command)

    def test_reuse_active_and_do_not_cancel(self):
        self.tc.builds.return_value = [build()]
        self.trigger.submit('Doris_CloudUT', {})
        self.tc.enqueue.assert_not_called()
        self.tc.request.assert_not_called()

    def test_retry_reuses_terminal_request(self):
        key = self.trigger.key + ':Doris_CloudUT:1'
        prior = build(state='finished', key=key)
        prior['status'] = 'FAILURE'
        self.tc.builds.return_value = [prior]
        self.trigger.submit('Doris_CloudUT', {})
        self.tc.enqueue.assert_not_called()

    def test_repeats_do_not_consume_a_later_ordinal(self):
        key = self.trigger.key + ':Doris_CloudUT:2'
        self.tc.builds.return_value = [build(key=key)]
        rows = self.trigger.submit('Doris_CloudUT', {}, count=2)
        self.assertEqual([row['id'] for row in rows], [10, 1])
        self.tc.enqueue.assert_called_once()

    def test_changed_head_stops_submission(self):
        self.github.pr.return_value['head']['sha'] = 'f' * 40
        with self.assertRaises(m.Error):
            self.trigger.submit('Doris_CloudUT', {})
        self.tc.enqueue.assert_not_called()

    def test_uncertain_post_is_not_retried(self):
        self.tc.enqueue.side_effect = m.Error('POST response lost')
        with self.assertRaises(m.Error):
            self.trigger.submit('Doris_CloudUT', {})
        self.tc.enqueue.assert_called_once()

    def test_wrong_revision_not_reused(self):
        stale = build()
        stale['revisions']['revision'][0]['version'] = 'f' * 40
        self.tc.builds.return_value = [stale]
        self.trigger.submit('Doris_CloudUT', {})
        self.tc.enqueue.assert_called_once()

    def test_compile_reuse_requires_requested_lanes(self):
        active = build()
        active['properties']['property'] = [{'name': 'regression_targets', 'value': 'external'}]
        self.tc.builds.return_value = [active]
        self.trigger.compile(['p0'])
        self.tc.enqueue.assert_called_once()

    def test_missing_regression_artifact_queues_compile_only(self):
        self.trigger.regression('p0', 1)
        args = self.tc.enqueue.call_args.args
        self.assertEqual(args[0], 'Doris_Compile')
        self.assertEqual(args[2]['regression_targets'], 'p0')

    def test_performance_release_branch_rejected(self):
        self.trigger.pr = copy.deepcopy(PR)
        self.trigger.pr['base']['ref'] = 'branch-4.2'
        with self.assertRaises(m.Error):
            self.trigger.performance(1)
        self.tc.enqueue.assert_not_called()

    def test_manifest_and_all_consumer_aliases(self):
        b = build(state='finished')
        doc, digest = m.validate_manifest(b, json.dumps(manifest()).encode(), PR)
        params = m.consumer_parameters(doc, digest, b)
        self.assertEqual(params['env.PR_HEAD_SHA'], 'a' * 40)
        self.assertEqual(params['env.EXPECTED_SOURCE_SHA'], 'c' * 40)
        self.assertEqual(params['env.COMPILE_BUILD_ID'], '1')
        self.assertEqual(params['env.PACKAGE_BUILD_NUMBER'], '1')
        self.assertEqual(params['env.CI_FANOUT_KEY'], '')
        self.assertEqual(params['env.PREPARED_VERIFIED_STATUS'], 'SUCCESS')

    def test_invalid_artifacts_rejected(self):
        for change in [lambda d: d['source'].update(pr_head_sha='f' * 40),
                       lambda d: d['producer'].update(packaged_in_build=2),
                       lambda d: d.update(expires_at='2000-01-01T00:00:00Z'),
                       lambda d: d['frameworkSource'].update(modified=True),
                       lambda d: d['packages']['binary.tar.gz'].update(sha256='bad')]:
            doc = manifest(); change(doc)
            with self.assertRaises(m.Error):
                m.validate_manifest(build(state='finished'), json.dumps(doc).encode(), PR)
        with self.assertRaises(m.Error):
            m.validate_manifest(build(), json.dumps(manifest()).encode(), PR)

    def test_temporary_vcs_root_rejected_before_post(self):
        tc = m.TeamCity('https://ci.example', 'test-token')
        tc.request = Mock(return_value={'vcs-root-entry': [{'vcs-root': {'id': 'temporary-debug-root'}}]})
        with self.assertRaises(m.Error):
            tc.enqueue('Doris_Compile', PR, {}, 'request')
        self.assertTrue(all(len(call.args) == 1 for call in tc.request.call_args_list))

    def test_expired_producer_causes_fresh_compile(self):
        expired = manifest(); expired['expires_at'] = '2000-01-01T00:00:00Z'
        self.tc.builds.return_value = [build(state='finished')]
        self.tc.artifact.return_value = json.dumps(expired).encode()
        self.trigger.regression('p0', 1)
        self.assertEqual(self.tc.enqueue.call_args.args[0], 'Doris_Compile')

    def test_native_regression_dependency_pinned_to_verified_producer(self):
        doc = manifest(); producer = build(state='finished')
        self.trigger.producer = Mock(return_value=(producer, doc, 'f' * 64))
        self.tc.request.return_value = {'artifact-dependency': [{
            'source-buildType': {'id': 'Doris_Compile'},
            'properties': {'property': [{'name': 'pathRules', 'value': 'regression/** => packages'}]}}]}
        self.trigger.regression('p0', 1)
        args = self.tc.enqueue.call_args.args
        self.assertEqual(args[0], 'Doris_P0')
        self.assertEqual(args[2]['env.CI_COMPILE_BUILD_ID'], '1')
        dependency = m.properties(args[4][0])
        self.assertEqual(dependency['revisionName'], 'buildNumber')
        self.assertEqual(dependency['revisionValue'], '1')
        self.assertEqual(dependency['pathRules'], 'regression/** => packages')

    def test_skip_authorization_and_run_permissions(self):
        gh = m.GitHub('test-token'); gh.request = Mock(return_value={'permission': 'read'})
        with self.assertRaises(m.Error):
            gh.authorize(EVENT, 'run', 'master')
        with self.assertRaises(m.Error):
            gh.authorize(EVENT, 'skip', 'master')
        allowed = copy.deepcopy(EVENT); allowed['comment']['user']['id'] = 9208457
        gh.authorize(allowed, 'skip', 'branch-4.2')
        gh.authorize(allowed, 'skip', 'master', 'check_coverage')
        with self.assertRaises(m.Error):
            gh.authorize(allowed, 'skip', 'master', 'buildall')


if __name__ == '__main__':
    unittest.main()
