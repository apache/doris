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

"""Exercise the existing shell entry points without a network or real credentials."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HELPER = Path(__file__).parents[1] / 'teamcity-utils.sh'
HEAD = 'a' * 40
MOCK = r'''
import json, os, re, sys
from pathlib import Path
root=Path(os.environ['FIXTURE_ROOT']); path=root/'state.json'; state=json.loads(path.read_text())
args=sys.argv[1:]; url=next(x for x in args if x.startswith('https://ci.example/app/rest/'))
route=url.split('/app/rest/',1)[1]; method=args[args.index('-X')+1] if '-X' in args else 'GET'
rows=state.get('builds',[])
def emit(value): print(json.dumps(value))
if method=='POST':
 assert route=='buildQueue'
 body=json.loads(args[args.index('--data-binary')+1]); state.setdefault('posts',[]).append(body)
 bid=100+len(state['posts']); body.update(id=bid,number=str(bid),buildTypeId=body['buildType']['id'],state='queued',status='UNKNOWN')
 body['revisions']['revision']=[{'version':body['revisions']['revision'][0]['version']}]
 state.setdefault('builds',[]).append(body); path.write_text(json.dumps(state))
 if state.get('lose_response'):
  state['lose_response']=False; path.write_text(json.dumps(state));sys.exit(1)
 emit({'id':bid});sys.exit()
if 'vcs-root-entries' in route:
 emit({'vcs-root-entry':[{'vcs-root':{'id':state.get('root','DorisMigrationValidation_ApacheDoris')}}]})
elif route.startswith('vcs-root-instances'):
 emit({'vcs-root-instance':[{'id':3,'vcs-root-id':'DorisMigrationValidation_ApacheDoris'}]})
elif '/artifact-dependencies' in route:
 emit({'artifact-dependency':[{'source-buildType':{'id':'Doris_Compile'},'properties':{'property':[{'name':'pathRules','value':'regression/** => packages'}]}}]})
elif '/artifacts/content/' in route:
 bid=route.split('/')[1].split(':')[1];name=route.split('/artifacts/content/',1)[1]
 emit(state['artifacts'][bid][name])
elif route.startswith('builds/id:'):
 bid=int(route.split(':')[1].split('?')[0]);emit(next(b for b in rows if b['id']==bid))
elif route.startswith(('builds?','buildQueue?')):
 if state.get('lookup_failure'): sys.exit(1)
 match=re.search(r'buildType:(?:\(id:)?([A-Za-z_]+)',route)
 if match:rows=[b for b in rows if b['buildTypeId']==match[1]]
 if route.startswith('buildQueue?'):rows=[b for b in rows if b['state']=='queued']
 else:
  rows=[b for b in rows if b['state']!='queued']
  if 'running:true' in route:rows=[b for b in rows if b['state']=='running']
  if 'state:finished' in route:rows=[b for b in rows if b['state']=='finished']
  if 'status:SUCCESS' in route:rows=[b for b in rows if b['status']=='SUCCESS']
 emit({'build':rows} if rows else {'count':0})
else: raise AssertionError(route)
'''


def build(bid=1, lane='Doris_CloudUT', state='running', head=HEAD, params=None):
    return {'id': bid, 'number': str(bid), 'buildTypeId': lane, 'state': state,
            'status': 'SUCCESS', 'branchName': 'pull/123',
            'revisions': {'revision': [{'version': head}]},
            'properties': {'property': [{'name': k, 'value': v} for k, v in (params or {}).items()]}}


def manifest():
    return {'source': {'repository': 'apache/doris', 'pr_number': 123, 'pr_head_sha': HEAD,
                       'target_branch': 'master', 'target_head_sha': 'b' * 40,
                       'checkout_sha': 'c' * 40, 'modified': False},
            'producer': {'compiled_in_build': 1, 'packaged_in_build': 1, 'image': 'image'},
            'trust_class': 'pr-merge', 'expires_at': '2999-01-01T00:00:00Z',
            'frameworkSource': {'modified': False}, 'tools': {'jdk': '17'},
            'unchangedTestTree': {'treeSha256': 'd' * 64},
            'packages': {name: {'sha256': 'e' * 64, 'bytes': 123}
                         for name in ['binary.tar.gz', 'cases.tar.gz']}}


class TeamCityUtilsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'mock.py').write_text(MOCK)
        self.save({})
        (self.root / 'event.json').write_text(json.dumps({'comment': {
            'id': 45, 'body': 'run cloudut', 'updated_at': '2026-11-11T00:00:00Z'}}))
        self.env = {**os.environ, 'TARGET_BRANCH': 'master', 'TEAMCITY_CURRENT_URL': 'https://ci.example',
                    'TEAMCITY_CURRENT_TOKEN': 'test-current-token', 'TEAMCITY_LEGACY_USER': 'test-legacy-user',
                    'TEAMCITY_LEGACY_PASSWORD': 'test-legacy-password', 'FIXTURE_ROOT': str(self.root),
                    'GITHUB_EVENT_PATH': str(self.root / 'event.json')}

    def save(self, value):
        (self.root / 'state.json').write_text(json.dumps(value))

    def state(self):
        return json.loads((self.root / 'state.json').read_text())

    def run_shell(self, command, mock=True, check=True, env=None):
        prefix = 'source "$1"\n'
        if mock:
            prefix += '''teamcity_request(){ python3 "$FIXTURE_ROOT/mock.py" "$@"; }
teamcity_current_pr(){ [[ ${PR_FRESH:-true} == true ]]; }
'''
        result = subprocess.run(['bash', '-c', prefix + command, 'test', str(HELPER)],
                                env={**self.env, **(env or {})}, text=True, capture_output=True)
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def trigger(self, lane='cloudut', count=1, **kwargs):
        return self.run_shell('trigger_or_skip_build true 123 master ' + HEAD + ' ' + lane + ' ' + str(count), **kwargs)

    def test_current_and_legacy_routes(self):
        for branch in ['master', 'branch-4.2', 'branch-4.1']:
            result = self.run_shell('echo "$TC_CURRENT ${comment_to_pipeline[compile]} ${conment_to_context[nonConcurrent]}"', env={'TARGET_BRANCH': branch})
            self.assertEqual(result.stdout.strip(), 'true Doris_Compile TeamCity/nonconcurrent')
        result = self.run_shell('echo "$TC_CURRENT ${comment_to_pipeline[compile]}"', env={'TARGET_BRANCH': 'branch-4.0'})
        self.assertEqual(result.stdout.strip(), 'false Doris_DorisCompile_Compile')

    def test_credentials_are_not_traced(self):
        for branch, expected, forbidden in [('master', 'test-current-token', 'test-legacy-password'),
                                             ('branch-4.0', 'test-legacy-password', 'test-current-token')]:
            result = self.run_shell('''curl(){ printf '%s\\n' "$@" > "$FIXTURE_ROOT/curl-args"; }
set -x
teamcity_request --fail https://ci.example
''', mock=False, env={'TARGET_BRANCH': branch})
            self.assertNotIn(expected, result.stdout + result.stderr)
            args = (self.root / 'curl-args').read_text()
            self.assertIn(expected, args)
            self.assertNotIn(forbidden, args)

    def test_status_token_is_not_traced(self):
        result = self.run_shell('curl(){ return 0; }; set -x; skip_build "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" feut',
                                mock=False, env={'GITHUB_TOKEN': 'test-github-token'})
        self.assertNotIn('test-github-token', result.stdout + result.stderr)

    def test_missing_private_token_has_no_public_fallback(self):
        result = self.run_shell('curl(){ exit 99; }; teamcity_request https://ci.example', mock=False,
                                check=False, env={'TEAMCITY_CURRENT_TOKEN': ''})
        self.assertEqual(result.returncode, 1)

    def test_exact_revision_submission(self):
        self.trigger()
        body = self.state()['posts'][0]
        self.assertEqual(body['buildType']['id'], 'Doris_CloudUT')
        self.assertEqual(body['branchName'], 'pull/123')
        self.assertEqual(body['properties']['property'][0]['name'], 'env.PR_NUMBER')

    def test_active_build_reused_without_cancel(self):
        self.save({'builds': [build()]})
        self.trigger()
        self.assertFalse(self.state().get('posts'))

    def test_other_revision_is_preserved_and_not_reused(self):
        self.save({'builds': [build(head='f' * 40)]})
        self.trigger()
        self.assertEqual(len(self.state()['posts']), 1)
        self.assertEqual(self.state()['builds'][0]['state'], 'running')

    def test_repeat_count_and_workflow_retry(self):
        self.trigger(count=2)
        self.trigger(count=2)
        self.assertEqual(len(self.state()['posts']), 2)

    def test_uncertain_post_reconciled_on_retry(self):
        self.save({'lose_response': True})
        self.assertNotEqual(self.trigger(check=False).returncode, 0)
        self.trigger()
        self.assertEqual(len(self.state()['posts']), 1)

    def test_lookup_failure_does_not_submit(self):
        self.save({'lookup_failure': True})
        self.assertNotEqual(self.trigger(check=False).returncode, 0)
        self.assertFalse(self.state().get('posts'))

    def test_changed_pr_does_not_submit(self):
        self.assertNotEqual(self.trigger(check=False, env={'PR_FRESH': 'false'}).returncode, 0)
        self.assertFalse(self.state().get('posts'))

    def test_temporary_configuration_rejected(self):
        self.save({'root': 'Doris_Performance_Schema70411_Debug'})
        self.assertNotEqual(self.trigger('performance', check=False).returncode, 0)
        self.assertFalse(self.state().get('posts'))

    def test_invalid_count_rejected(self):
        for count in [0, 21, 'abc']:
            self.assertNotEqual(self.trigger(count=count, check=False).returncode, 0)
        self.assertFalse(self.state().get('posts'))

    def test_missing_artifact_triggers_matching_compile(self):
        self.trigger('p0')
        body = self.state()['posts'][0]
        self.assertEqual(body['buildType']['id'], 'Doris_Compile')
        params = {p['name']: p['value'] for p in body['properties']['property']}
        self.assertEqual(params['regression_targets'], 'p0')

    def test_compile_active_scope_must_cover_requested_lane(self):
        self.save({'builds': [build(lane='Doris_Compile', params={'regression_targets': 'external', 'is_debug': 'false'})]})
        self.trigger('p0')
        self.assertEqual(len(self.state()['posts']), 1)

    def with_producer(self, doc):
        self.save({'builds': [build(lane='Doris_Compile', state='finished')],
                   'artifacts': {'1': {'regression/manifest.json': doc}}})

    def test_native_artifact_binding(self):
        self.with_producer(manifest())
        self.trigger('p0')
        body = self.state()['posts'][0]
        self.assertEqual(body['buildType']['id'], 'Doris_P0')
        params = {p['name']: p['value'] for p in body['properties']['property']}
        self.assertEqual(params['env.CI_COMPILE_BUILD_ID'], '1')
        self.assertEqual(params['env.EXPECTED_SOURCE_SHA'], 'c' * 40)
        self.assertEqual(params['env.PACKAGE_BUILD_NUMBER'], '1')
        self.assertEqual(params['env.CI_FANOUT_KEY'], '')
        dep = body['custom-artifact-dependencies']['artifact-dependency'][0]
        self.assertEqual({p['name']: p['value'] for p in dep['properties']['property']}['revisionValue'], '1')

    def test_invalid_manifest_not_used(self):
        for change in [lambda d: d['source'].update(pr_head_sha='f' * 40),
                       lambda d: d['producer'].update(packaged_in_build=2),
                       lambda d: d['frameworkSource'].update(modified=True)]:
            doc = manifest(); change(doc); self.with_producer(doc)
            self.assertNotEqual(self.trigger('p0', check=False).returncode, 0)
            self.assertFalse(self.state().get('posts'))

    def test_expired_manifest_causes_recompile(self):
        doc = manifest(); doc['expires_at'] = '2000-01-01T00:00:00Z'; self.with_producer(doc)
        self.trigger('p0')
        self.assertEqual(self.state()['posts'][0]['buildType']['id'], 'Doris_Compile')

    def test_removed_lane_has_no_legacy_fallback(self):
        self.assertNotEqual(self.trigger('p1', check=False).returncode, 0)
        self.assertFalse(self.state().get('posts'))


if __name__ == '__main__':
    unittest.main()
