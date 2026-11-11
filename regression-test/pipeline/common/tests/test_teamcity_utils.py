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
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import textwrap
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
elif route.startswith('builds/id:'):
 bid=int(route.split(':')[1].split('?')[0]);emit(next(b for b in rows if b['id']==bid))
else: raise AssertionError(route)
'''


def build(bid=1, lane='Doris_CloudUT', state='running', head=HEAD, params=None):
    return {'id': bid, 'number': str(bid), 'buildTypeId': lane, 'state': state,
            'status': 'SUCCESS', 'branchName': 'pull/123',
            'revisions': {'revision': [{'version': head}]},
            'properties': {'property': [{'name': k, 'value': v} for k, v in (params or {}).items()]}}


class TeamCityUtilsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'mock.py').write_text(MOCK)
        (self.root / 'status_mock.py').write_text('''import json, os, sys
from pathlib import Path
p=Path(os.environ['FIXTURE_ROOT'])/'statuses.json'
rows=json.loads(p.read_text()) if p.exists() else []
assert '-d' in sys.argv and '-X' in sys.argv
if os.environ.get('STATUS_FAIL')=='true': sys.exit(22)
rows.append(json.loads(sys.argv[sys.argv.index('-d')+1]));p.write_text(json.dumps(rows))
''')
        self.save({})
        (self.root / 'event.json').write_text(json.dumps({'comment': {
            'id': 45, 'body': 'run cloudut', 'updated_at': '2026-11-11T00:00:00Z'}}))
        self.env = {**os.environ, 'TARGET_BRANCH': 'master', 'TEAMCITY_CURRENT_URL': 'https://ci.example',
                    'TEAMCITY_CURRENT_TOKEN': 'test-current-token', 'GITHUB_TOKEN': 'test-github-token', 'FIXTURE_ROOT': str(self.root),
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
curl(){ python3 "$FIXTURE_ROOT/status_mock.py" "$@"; }
'''
        result = subprocess.run(['bash', '-c', prefix + command, 'test', str(HELPER)],
                                env={**self.env, **(env or {})}, text=True, capture_output=True)
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def trigger(self, lane='cloudut', count=1, **kwargs):
        return self.run_shell('trigger_or_skip_build true 123 master ' + HEAD + ' ' + lane + ' ' + str(count), **kwargs)

    def skipped_contexts(self):
        p = self.root / 'statuses.json'
        return [row['context'] for row in json.loads(p.read_text())] if p.exists() else []

    def test_unrelated_files_skip_without_submitting(self):
        self.run_shell('trigger_or_skip_build false 123 master ' + HEAD + ' feut 1')
        self.assertEqual(self.skipped_contexts(), ['TeamCity/feut'])
        self.assertFalse(self.state().get('posts'))

    def test_compile_skip_covers_downstream_checks(self):
        self.run_shell('trigger_or_skip_build false 123 master ' + HEAD + ' compile 1')
        self.assertEqual(self.skipped_contexts(), ['TeamCity/compile', 'TeamCity/p0', 'TeamCity/cloud_p0',
                                                  'TeamCity/vault_p0', 'TeamCity/external', 'TeamCity/nonconcurrent'])
        self.assertFalse(self.state().get('posts'))

    def test_failed_skip_and_stale_head_do_not_succeed(self):
        for env in [{'STATUS_FAIL': 'true'}, {'PR_FRESH': 'false'}]:
            result = self.run_shell('trigger_or_skip_build false 123 master ' + HEAD + ' feut 1', check=False, env=env)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.skipped_contexts())

    def file_filter_outputs(self, files, failed=False):
        workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        block = workflow.split('    - name: "Check if pr need run build"\n', 1)[1].split('    # - name:', 1)[0]
        script = textwrap.dedent(block.split('      run: |\n', 1)[1])
        github_helper = HELPER.with_name('github-utils.sh')
        override = 'source "' + str(github_helper) + '"\n'
        override += '_get_pr_changed_files(){ return 1; }' if failed else '_get_pr_changed_files(){ return 0; }'
        script = script.replace('source regression-test/pipeline/common/github-utils.sh', override)
        script = script.replace('${{ steps.parse.outputs.PULL_REQUEST_NUM }}', '123')
        for name in ['all_files', 'modified_files']:
            (self.root / name).write_text('\n'.join(files) + '\n')
        for name in ['added_files', 'removed_files']:
            (self.root / name).write_text('')
        output = self.root / 'filter-output'; output.write_text('')
        result = subprocess.run(['bash', '-eo', 'pipefail', '-c', script], cwd=self.root,
                                env={**self.env, 'GITHUB_OUTPUT': str(output)}, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return dict(line.split('=', 1) for line in output.read_text().splitlines())

    def test_original_file_filters_and_workflow_wiring(self):
        for files, expected in [(['fe/fe-core/src/main/java/Test.java'], {'feut', 'p0', 'cloud_p0', 'vault_p0', 'external', 'nonconcurrent', 'performance', 'compile'}),
                                (['cloud/src/meta-service/meta_service.cpp'], {'cloudut', 'cloud_p0', 'vault_p0', 'compile'}),
                                (['README.md'], set()),
                                (['regression-test/pipeline/p0/conf/regression-conf.groovy'], set())]:
            flags = self.file_filter_outputs(files)
            workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
            selected = set()
            for lane in ['compile', 'feut', 'beut', 'cloudut', 'p0', 'cloud_p0', 'vault_p0', 'external', 'nonconcurrent', 'performance']:
                block = workflow.split('    - name: "Trigger ' + lane + '"\n', 1)[1].split('    - name:', 1)[0]
                script = textwrap.dedent(block.split('      run: |\n', 1)[1])
                script = script.replace('source ./regression-test/pipeline/common/teamcity-utils.sh', 'trigger_or_skip_build(){ echo "SELECTED=$1"; }')
                values = {'COMMENT_TRIGGER_TYPE': lane, 'PULL_REQUEST_NUM': '123', 'TARGET_BRANCH': 'master',
                          'COMMIT_ID_FROM_TRIGGER': HEAD, 'COMMENT_REPEAT_TIMES': '1'}
                for key, value in flags.items(): script = script.replace('${{ steps.changes.outputs.' + key + ' }}', value)
                for key, value in values.items(): script = script.replace('${{ steps.parse.outputs.' + key + ' }}', value)
                result = subprocess.run(['bash', '-eo', 'pipefail', '-c', script], text=True, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                if 'SELECTED=true' in result.stdout: selected.add(lane)
            self.assertEqual(selected, expected)

    def test_file_lookup_does_not_trace_github_token(self):
        helper = HELPER.with_name('github-utils.sh')
        script = 'source "$1"\n' + '''curl(){
            if [[ "$*" == *--version* ]]; then return 0;
            elif [[ "$*" == *'/files?'* ]]; then echo '[{"filename":"README.md","status":"modified"}]';
            else echo '{"changed_files":1}'; fi
        }
        _get_pr_changed_files 123
'''
        result = subprocess.run(['bash', '-c', script, 'test', str(helper)], cwd=self.root,
                                env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('test-github-token', result.stdout + result.stderr)

    def test_failed_file_lookup_does_not_select_skip(self):
        flags = self.file_filter_outputs([], failed=True)
        self.assertTrue(flags and all(value == 'true' for value in flags.values()))

    def test_keywords_match_required_checks(self):
        repo = HELPER.parents[3]
        required = set(re.findall(r'- (TeamCity/\S+)', (repo / '.asf.yaml').read_text()))
        result = self.run_shell('for lane in "${!comment_to_pipeline[@]}"; do echo "$lane ${conment_to_context[$lane]}"; done')
        mappings = dict(line.split() for line in result.stdout.splitlines())
        self.assertEqual(set(mappings.values()), required | {'TeamCity/external', 'TeamCity/performance'})
        self.assertTrue(all(context == 'TeamCity/' + lane for lane, context in mappings.items()))
        workflow = (repo / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        commands = re.search(r'reg="\^run \(([^)]+)\)', workflow).group(1).split('|')
        self.assertEqual(set(commands), set(mappings) | {'buildall'})

    def parse_comment(self, command, branch='master', permission='write', check=True, user_id='27881198'):
        workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        block = workflow.split('    - name: "Parse PR comment"\n', 1)[1].split('    - name:', 1)[0]
        script = textwrap.dedent(block.split('      run: |\n', 1)[1])
        script = script.replace('${{ github.event.issue.pull_request.url }}', 'https://api.github.com/repos/apache/doris/pulls/123')
        script = script.replace('${{ github.repository }}', 'apache/doris').replace('${{ github.event.comment.user.login }}', 'tester')
        output = self.root / 'workflow-output'; output.write_text('')
        prefix = 'curl(){ printf "%s\\n" "$MOCK_GITHUB_RESPONSE"; };\n'
        result = subprocess.run(['bash', '-eo', 'pipefail', '-c', prefix + script], text=True, capture_output=True,
                                env={**self.env, 'COMMENT_BODY': command, 'COMMENT_USER_ID': user_id,
                                     'GITHUB_TOKEN': 'test-github-token', 'GITHUB_OUTPUT': str(output),
                                     'GITHUB_ENV': str(self.root / 'workflow-env'),
                                     'MOCK_GITHUB_RESPONSE': json.dumps({'head': {'sha': HEAD}, 'base': {'ref': branch}, 'permission': permission})})
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result, output.read_text()

    def test_workflow_parses_exact_check_keywords(self):
        for lane in ['compile', 'feut', 'beut', 'cloudut', 'p0', 'cloud_p0', 'vault_p0',
                     'external', 'nonconcurrent', 'performance', 'buildall']:
            result, output = self.parse_comment('run ' + lane + ' 2')
            self.assertIn('COMMENT_TRIGGER_TYPE=' + lane + '\n', output)
            self.assertIn('COMMENT_REPEAT_TIMES=2\n', output)
        for command in ['run feut-extra', 'please run feut', 'run nonConcurrent', 'run p1', 'run cloud_p1', 'run check_coverage']:
            result, output = self.parse_comment(command, check=False)
            self.assertNotIn('COMMENT_TRIGGER_TYPE=', output)

    def test_skip_buildall_authorization_is_preserved(self):
        _, output = self.parse_comment('skip buildall')
        self.assertIn('comment_skip=true', output)
        _, output = self.parse_comment('skip buildall', user_id='123456', check=False)
        self.assertNotIn('comment_skip=true', output)
        _, output = self.parse_comment('skip buildall', branch='branch-4.2', user_id='9208457')
        self.assertIn('comment_skip=true', output)

    def test_skip_buildall_statuses(self):
        workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        block = workflow.split('    - name: "Skip buildall"\n', 1)[1]
        script = textwrap.dedent(block.split('      run: |\n', 1)[1])
        script = script.replace('source ./regression-test/pipeline/common/teamcity-utils.sh', ':')
        script = script.replace('${{ steps.parse.outputs.COMMIT_ID_FROM_TRIGGER }}', HEAD)
        for branch in ['master', 'branch-4.2', 'branch-4.1']:
            (self.root / 'statuses.json').write_text('[]')
            self.run_shell(script, env={'TARGET_BRANCH': branch})
            expected = {'TeamCity/' + lane for lane in ['compile', 'feut', 'beut', 'cloudut', 'p0', 'cloud_p0', 'vault_p0', 'external', 'nonconcurrent']}
            if branch == 'master': expected.add('TeamCity/performance')
            self.assertEqual(set(self.skipped_contexts()), expected)
            self.assertFalse(self.state().get('posts'))

    def test_workflow_branch_and_permission_checks(self):
        for branch in ['branch-4.2', 'branch-4.1']:
            result, _ = self.parse_comment('run performance', branch=branch, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.parse_comment('run buildall', branch=branch)
            self.parse_comment('run feut', branch=branch)
        for branch, permission in [('branch-4.0', 'write'), ('master', 'read')]:
            result, _ = self.parse_comment('run feut', branch=branch, permission=permission, check=False)
            self.assertNotEqual(result.returncode, 0)

    def test_credentials_are_not_traced(self):
        result = self.run_shell('''curl(){ printf '%s\\n' "$@" > "$FIXTURE_ROOT/curl-args"; }
set -x
teamcity_request --fail https://ci.example
''', mock=False)
        self.assertNotIn('test-current-token', result.stdout + result.stderr)
        args = (self.root / 'curl-args').read_text()
        self.assertIn('Authorization: Bearer test-current-token', args)
        self.assertNotIn('--user', args)

    def test_retired_branch_has_no_submission(self):
        result = self.run_shell('trigger_or_skip_build true 123 branch-4.0 ' + HEAD + ' feut 1', check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.state().get('posts'))

    def test_release_branches_reject_performance(self):
        for branch in ['branch-4.2', 'branch-4.1']:
            result = self.run_shell('trigger_or_skip_build true 123 ' + branch + ' ' + HEAD + ' performance 1', check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.state().get('posts'))

    def test_status_token_is_not_traced(self):
        result = self.run_shell('curl(){ return 0; }; set -x; skip_build "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" feut',
                                mock=False, env={'GITHUB_TOKEN': 'test-github-token'})
        self.assertNotIn('test-github-token', result.stdout + result.stderr)

    def test_missing_private_token_has_no_public_fallback(self):
        result = self.run_shell('curl(){ exit 99; }; teamcity_request https://ci.example', mock=False,
                                check=False, env={'TEAMCITY_CURRENT_TOKEN': ''})
        self.assertEqual(result.returncode, 1)

    def test_regressions_submit_directly_with_only_pr_parameters(self):
        for lane, pipeline in [('p0', 'Doris_P0'), ('cloud_p0', 'Doris_CloudP0'),
                               ('vault_p0', 'Doris_VaultP0'), ('external', 'Doris_External'),
                               ('nonconcurrent', 'Doris_NonConcurrent')]:
            self.save({})
            self.trigger(lane)
            body = self.state()['posts'][0]
            self.assertEqual(body['buildType']['id'], pipeline)
            self.assertEqual({p['name']: p['value'] for p in body['properties']['property']},
                             {'env.PR_NUMBER': '123', 'env.CI_BRANCH': 'master'})
            self.assertNotIn('custom-artifact-dependencies', body)
            self.assertEqual(len(self.state()['posts']), 1)

    def test_exact_revision_submission(self):
        self.trigger()
        body = self.state()['posts'][0]
        self.assertEqual(body['buildType']['id'], 'Doris_CloudUT')
        self.assertEqual(body['branchName'], 'pull/123')
        self.assertEqual(body['properties']['property'][0]['name'], 'env.PR_NUMBER')

    def test_active_build_does_not_prevent_submission(self):
        self.save({'builds': [build()]})
        self.trigger()
        self.assertEqual(len(self.state()['posts']), 1)
        self.assertEqual(self.state()['builds'][0]['state'], 'running')

    def test_other_revision_is_preserved_and_not_reused(self):
        self.save({'builds': [build(head='f' * 40)]})
        self.trigger()
        self.assertEqual(len(self.state()['posts']), 1)
        self.assertEqual(self.state()['builds'][0]['state'], 'running')

    def test_each_request_is_submitted_to_teamcity(self):
        self.trigger(count=2)
        self.trigger(count=2)
        self.assertEqual(len(self.state()['posts']), 4)
        for body in self.state()['posts']:
            self.assertNotIn('env.CI_COMMENT_REQUEST_KEY', {p['name'] for p in body['properties']['property']})

    def test_uncertain_post_does_not_automatically_retry(self):
        self.save({'lose_response': True})
        self.assertNotEqual(self.trigger(check=False).returncode, 0)
        self.assertEqual(len(self.state()['posts']), 1)

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

    def test_removed_lane_has_no_legacy_fallback(self):
        self.assertNotEqual(self.trigger('p1', check=False).returncode, 0)
        self.assertFalse(self.state().get('posts'))


if __name__ == '__main__':
    unittest.main()
