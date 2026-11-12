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

"""Check PR/commit binding after TeamCity downloads native dependencies."""
import hashlib
import io
import importlib.util
import json
from pathlib import Path
import unittest
import tempfile
from contextlib import redirect_stdout
from unittest.mock import patch

from test_teamcity_utils import HEAD, build

spec = importlib.util.spec_from_file_location('artifacts', Path(__file__).parents[1] / 'teamcity_artifacts.py')
a = importlib.util.module_from_spec(spec)
spec.loader.exec_module(a)


class ArtifactTests(unittest.TestCase):
    def compile_fixture(self):
        manifest = {
            'schema': 1, 'trust_class': 'pr-merge',
            'source': {'repository': 'apache/doris', 'pr_number': 123, 'target_branch': 'master',
                       'pr_head_sha': HEAD, 'target_head_sha': 'b' * 40, 'checkout_sha': 'c' * 40,
                       'modified': False},
            'producer': {'compiled_in_build': '1', 'packaged_in_build': '1', 'image': 'test-image'},
            'frameworkSource': {'modified': False, 'checkout_sha': 'c' * 40},
            'unchangedTestTree': {'unchanged': True, 'treeSha256': 'd' * 64},
            'publication_state': 'uploaded-awaiting-teamcity-terminal-success',
            'consumer_gate': {'teamcity_status': 'SUCCESS', 'pr_head_must_match': True, 'verify_sha256': True},
            'expires_at': '2099-01-01T00:00:00Z',
            'packages': {name: {'sha256': char * 64, 'bytes': size} for name, char, size in
                         [('binary.tar.gz', 'e', 1234), ('cases.tar.gz', 'f', 5678)]},
        }
        return {'builds': [build(lane='Doris_Compile', state='finished')],
                'manifest': json.dumps(manifest, indent=2) + '\n'}


    def setUp(self):
        self.fixture = self.compile_fixture()
        self.env = {'CI_REQUESTED_PR_HEAD': HEAD}

    def bind(self):
        return a.consumer_parameters(self.fixture['builds'][0], self.fixture['manifest'].encode(),
                                     '123', HEAD, 'master', self.env)

    def test_binds_existing_runner_aliases(self):
        params = self.bind()
        self.assertEqual(len(params), 43)
        self.assertEqual(params['env.CI_COMPILE_BUILD_ID'], '1')
        self.assertEqual(params['env.PR_HEAD_SHA'], HEAD)
        self.assertEqual(params['env.SOURCE_SHA'], 'c' * 40)
        self.assertEqual(params['env.PR_BASE_SHA'], 'b' * 40)
        self.assertEqual(params['env.CI_PREPARED_MANIFEST_SHA256'],
                         hashlib.sha256(self.fixture['manifest'].encode()).hexdigest())

    def test_rejects_wrong_native_producer(self):
        for key, value in [('buildTypeId', 'Doris_FEUT'), ('state', 'running'),
                           ('status', 'FAILURE'), ('branchName', 'pull/124'),
                           ('canceledInfo', {'text': 'canceled'}),
                           ('revisions', {'revision': [{'version': 'f' * 40}]})]:
            with self.subTest(key=key):
                self.fixture = self.compile_fixture()
                self.fixture['builds'][0][key] = value
                with self.assertRaises(ValueError): self.bind()

    def test_rejects_wrong_manifest_identity(self):
        for section, key, value in [('source', 'pr_number', 124), ('source', 'pr_head_sha', 'f' * 40),
                                    ('source', 'target_branch', 'branch-4.2'),
                                    ('source', 'repository', 'other/repo'),
                                    ('producer', 'compiled_in_build', '2'),
                                    ('producer', 'packaged_in_build', '2')]:
            with self.subTest(key=key):
                self.fixture = self.compile_fixture()
                manifest = json.loads(self.fixture['manifest'])
                manifest[section][key] = value
                self.fixture['manifest'] = json.dumps(manifest)
                with self.assertRaises(ValueError): self.bind()

    def test_rejects_changed_request_commit(self):
        self.env['CI_REQUESTED_PR_HEAD'] = 'f' * 40
        with self.assertRaisesRegex(ValueError, 'Requested commit'): self.bind()

    def test_preserves_exact_compile_fanout(self):
        params = self.bind()
        self.env.update({k[4:]: v for k, v in params.items()})
        self.env['CI_FANOUT_KEY'] = 'compile:1:p0'
        self.assertEqual(params, self.bind())
        for key in ['CI_COMPILE_BUILD_ID', 'CI_PREPARED_MANIFEST_SHA256', 'EXPECTED_BINARY_SHA256']:
            original = self.env[key]
            self.env[key] = 'wrong'
            with self.assertRaisesRegex(ValueError, 'fanout binding differs'): self.bind()
            self.env[key] = original

    def test_main_reads_the_resolved_dependency_of_the_current_build(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = Path(temp) / 'manifest.json'
            manifest.write_text(self.fixture['manifest'])
            properties = Path(temp) / 'build.properties'
            properties.write_text('teamcity.build.id=2\nteamcity.auth.userId=build-user\n'
                                  'teamcity.auth.password=temporary-password\n')
            current = {'branchName': 'pull/123', 'revisions': {'revision': [{'version': HEAD}]},
                       'artifact-dependencies': {'build': self.fixture['builds']}}
            env = {'PR_NUMBER': '123', 'CI_BRANCH': 'master', 'CI_REQUESTED_PR_HEAD': HEAD,
                   'TEAMCITY_BUILD_PROPERTIES_FILE': str(properties)}
            args = ['binding', '--server', 'https://tc.example', '--manifest', str(manifest)]
            with patch.dict(a.os.environ, env, clear=True), patch.object(a.sys, 'argv', args), \
                    patch.object(a.os, 'getuid', return_value=1000), \
                    patch.object(a.urllib.request, 'build_opener') as opener:
                output = io.StringIO()
                opener.return_value.open.return_value = io.StringIO(json.dumps(current))
                with redirect_stdout(output): a.main()
                self.assertEqual(output.getvalue().count('##teamcity[setParameter'), 43)
                self.assertNotIn('temporary-password', output.getvalue())
                request = opener.return_value.open.call_args.args[0]
                self.assertTrue(request.full_url.startswith('https://tc.example/app/rest/builds/id:2?'))
                self.assertEqual(opener.return_value.open.call_count, 1)
                current['revisions']['revision'][0]['version'] = 'f' * 40
                opener.return_value.open.return_value = io.StringIO(json.dumps(current))
                output = io.StringIO()
                with redirect_stdout(output), self.assertRaises(ValueError): a.main()
                self.assertEqual(output.getvalue(), '')

    def test_service_message_escaping(self):
        self.assertEqual(a.escape("a|'[b]\n\r"), "a|||'|[b|]|n|r")


if __name__ == '__main__':
    unittest.main()
