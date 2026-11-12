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

"""TeamCity-side artifact resolution; no real server or package downloads."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from test_teamcity_utils import HEAD, build

spec = importlib.util.spec_from_file_location('artifacts', Path(__file__).parents[1] / 'teamcity_artifacts.py')
a = importlib.util.module_from_spec(spec)
spec.loader.exec_module(a)


class FakeAPI:
    def __init__(self, fixture):
        self.fixture = fixture
        self.requests = []

    def get(self, path):
        self.requests.append(path)
        f = self.fixture
        if path.startswith('builds/id:2?'):
            return f['consumer']
        if path.startswith('builds/id:1?'):
            return f['builds'][0]
        if '/artifacts/children/' in path:
            return f['artifacts']
        if path.startswith('builds?'):
            if f.get('pages'):
                return f['pages'].pop(0)
            return {'build': f['builds']}
        raise AssertionError(path)

    def read(self, path):
        self.requests.append(path)
        if self.fixture.get('missing_manifest'):
            raise OSError('Manifest missing')
        return self.fixture['manifest'].encode()

    def download(self, bid, name, destination, metadata):
        self.requests.append(name)
        if self.fixture.get('bad_download'):
            destination.write_bytes(b'partial')
            raise ValueError('Artifact size/SHA256 mismatch')
        destination.write_bytes(b'test')


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
                'manifest': json.dumps(manifest, indent=2) + '\n',
                'artifacts': {'file': [{'name': k, 'size': v['bytes']} for k, v in manifest['packages'].items()]}}


    def setUp(self):
        self.fixture = self.compile_fixture()
        self.fixture['consumer'] = build(bid=2, lane='Doris_P0')
        self.env = {'PR_NUMBER': '123', 'CI_REQUESTED_PR_HEAD': HEAD, 'CI_BRANCH': 'master',
                    'CI_COMPILE_BUILD_ID': '', 'CI_PREPARED_BUILD_NUMBER': 'UNBOUND'}
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def resolve(self):
        self.api = FakeAPI(self.fixture)
        return a.resolve(self.api, self.fixture['consumer'], self.env)

    def test_all_five_consumers_resolve_current_pr_revision(self):
        for lane in a.DESTINATIONS:
            self.fixture['consumer']['buildTypeId'] = lane
            bid, raw, params = self.resolve()
            self.assertEqual(bid, '1')
            self.assertEqual(len(params), 43)
            self.assertEqual(params['env.CI_PREPARED_BUILD_NUMBER'], '1')
            self.assertEqual(params['env.PR_HEAD_SHA'], HEAD)
            self.assertEqual(params['env.SOURCE_SHA'], 'c' * 40)
            self.assertEqual(params['env.PR_BASE_SHA'], 'b' * 40)
            self.assertEqual(params['env.CI_PREPARED_MANIFEST_SHA256'], hashlib.sha256(raw).hexdigest())

    def test_missing_or_other_revision_never_rebuilds_compile(self):
        for builds in [[], [build(lane='Doris_Compile', state='finished', head='f' * 40)]]:
            self.fixture['builds'] = builds
            with self.assertRaisesRegex(ValueError, 'No successful Compile'):
                self.resolve()
            self.assertTrue(all(p.startswith('builds?') for p in self.api.requests))

    def test_rejects_invalid_source_manifest_or_package(self):
        for change in ['missing_manifest', 'missing_cases', 'size', 'producer', 'head', 'pr', 'branch',
                       'expiry', 'sha', 'modified', 'proof', 'gate', 'canceled', 'running', 'failed']:
            with self.subTest(change=change):
                self.fixture = self.compile_fixture()
                self.fixture['consumer'] = build(bid=2, lane='Doris_P0')
                m = json.loads(self.fixture['manifest'])
                if change == 'missing_manifest': self.fixture[change] = True
                elif change == 'missing_cases': self.fixture['artifacts']['file'].pop()
                elif change == 'size': self.fixture['artifacts']['file'][0]['size'] += 1
                elif change == 'producer': m['producer']['packaged_in_build'] = '2'
                elif change == 'head': m['source']['pr_head_sha'] = 'f' * 40
                elif change == 'pr': m['source']['pr_number'] = 124
                elif change == 'branch': m['source']['target_branch'] = 'branch-4.2'
                elif change == 'expiry': m['expires_at'] = '2000-01-01T00:00:00Z'
                elif change == 'sha': m['packages']['binary.tar.gz']['sha256'] = 'invalid'
                elif change == 'modified': m['frameworkSource']['modified'] = True
                elif change == 'proof': m['unchangedTestTree']['unchanged'] = False
                elif change == 'gate': m['consumer_gate']['verify_sha256'] = False
                elif change == 'canceled': self.fixture['builds'][0]['canceledInfo'] = {'text': 'canceled'}
                elif change == 'running': self.fixture['builds'][0]['state'] = 'running'
                elif change == 'failed': self.fixture['builds'][0]['status'] = 'FAILURE'
                self.fixture['manifest'] = json.dumps(m)
                with self.assertRaises((ValueError, OSError)): self.resolve()

    def test_automatic_fanout_keeps_its_exact_producer(self):
        _, _, params = self.resolve()
        self.env.update({k[4:]: v for k, v in params.items()})
        self.env['CI_FANOUT_KEY'] = 'compile:1:p0'
        self.resolve()
        self.assertFalse(any(p.startswith('builds?') for p in self.api.requests))
        self.env['EXPECTED_BINARY_SHA256'] = 'f' * 64
        with self.assertRaisesRegex(ValueError, 'fanout binding differs'): self.resolve()
        self.env['CI_COMPILE_BUILD_ID'] = ''
        with self.assertRaisesRegex(ValueError, 'missing its producer'): self.resolve()

    def test_requested_identity_must_match_actual_consumer(self):
        for key, value in [('PR_NUMBER', '124'), ('CI_REQUESTED_PR_HEAD', 'f' * 40), ('CI_BRANCH', 'branch-4.0')]:
            previous = self.env[key]; self.env[key] = value
            with self.assertRaises(ValueError): self.resolve()
            self.env[key] = previous

    def test_pagination_and_loop_or_external_page_rejection(self):
        self.fixture['pages'] = [{'build': [], 'nextHref': '/app/rest/builds?locator=start:100'},
                                 {'build': self.fixture['builds']}]
        self.resolve()
        for next_href in ['https://other.example/app/rest/builds', '/app/rest/builds?locator=start:100']:
            self.fixture['pages'] = [{'build': [], 'nextHref': next_href}] * 3
            with self.assertRaises(ValueError): self.resolve()

    def test_properties_are_unescaped_without_exporting_credentials(self):
        p = self.root / 'system.properties'
        p.write_text(r'teamcity.auth.password=p\:a\\ss\u0021' + '\n')
        self.assertEqual(a.build_properties(p)['teamcity.auth.password'], 'p:a\\ss!')
        with self.assertRaisesRegex(ValueError, 'redirect rejected'):
            a.NoRedirect().redirect_request(None)
        with self.assertRaisesRegex(ValueError, 'HTTPS'):
            a.TeamCity('http://example.com', {})

    def test_download_hash_and_length_are_checked(self):
        client = a.TeamCity('https://ci.example', {'teamcity.auth.userId': 'build', 'teamcity.auth.password': 'secret'})
        for data in [b'test', b'bad', b'testextra']:
            destination = self.root / ('artifact-' + str(len(data)))
            with patch.object(client, 'open', return_value=io.BytesIO(data)):
                metadata = {'bytes': 4, 'sha256': hashlib.sha256(b'test').hexdigest()}
                if data == b'test': client.download('1', 'binary.tar.gz', destination, metadata)
                else:
                    with self.assertRaises(ValueError): client.download('1', 'binary.tar.gz', destination, metadata)

    def run_main(self):
        props = self.root / 'system.properties'
        props.write_text('teamcity.build.id=2\nteamcity.auth.userId=test-user\nteamcity.auth.password=test-secret\n')
        env_file = self.root / 'binding.env'
        output = io.StringIO()
        previous = Path.cwd()
        try:
            os.chdir(self.root)
            with patch.dict(os.environ, {**self.env, 'TEAMCITY_BUILD_PROPERTIES_FILE': str(props)}, clear=True), \
                 patch.object(a, 'TeamCity', return_value=FakeAPI(self.fixture)), \
                 patch.object(a.os, 'getuid', return_value=1000), \
                 patch.object(a.sys, 'argv', ['preflight', '--server', 'https://ci.example', '--environment', str(env_file)]), \
                 redirect_stdout(output):
                a.main()
        finally:
            os.chdir(previous)
            self.output = output.getvalue()
        return env_file

    def test_success_publishes_parameters_only_after_download(self):
        env_file = self.run_main()
        self.assertEqual(self.output.count('##teamcity[setParameter'), 43)
        self.assertTrue((self.root / 'packages/cases.tar.gz').exists())
        self.assertIn('export CI_COMPILE_BUILD_ID=1', env_file.read_text())
        self.assertNotIn('test-secret', self.output + env_file.read_text())
        self.assertEqual(env_file.stat().st_mode & 0o777, 0o600)

    def test_failed_download_does_not_publish_or_replace_existing_packages(self):
        (self.root / 'packages').mkdir()
        (self.root / 'packages/old').write_text('previous')
        self.fixture['bad_download'] = True
        with self.assertRaises(ValueError): self.run_main()
        self.assertNotIn('##teamcity[setParameter', self.output)
        self.assertFalse((self.root / 'binding.env').exists())
        self.assertTrue((self.root / 'packages/old').exists())
        self.assertFalse((self.root / '.packages-2').exists())

    def test_tool_paths_cannot_escape_staging(self):
        m = json.loads(self.fixture['manifest'])
        m['tools'] = {'tools/../../outside': {'sha256': 'f' * 64, 'bytes': 4}}
        self.fixture['manifest'] = json.dumps(m)
        with self.assertRaisesRegex(ValueError, 'Unsafe artifact path'): self.run_main()
        self.assertFalse((self.root / 'outside').exists())


if __name__ == '__main__':
    unittest.main()
