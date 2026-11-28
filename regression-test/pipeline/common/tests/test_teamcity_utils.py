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
FC_TRIGGER_URL = 'https://doris-c-trigger-mtjzjrppvu.cn-hongkong.fcapp.run/trigger'


class TeamCityUtilsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'status_mock.py').write_text('''import json, os, sys
from pathlib import Path
p=Path(os.environ['FIXTURE_ROOT'])/'statuses.json'
rows=json.loads(p.read_text()) if p.exists() else []
assert '-d' in sys.argv and '-X' in sys.argv
if os.environ.get('STATUS_FAIL')=='true': sys.exit(22)
rows.append(json.loads(sys.argv[sys.argv.index('-d')+1]));p.write_text(json.dumps(rows))
''')
        self.plan = self.root / 'fc-lanes'
        self.env = {**os.environ, 'TARGET_BRANCH': 'master', 'GITHUB_TOKEN': 'test-github-token',
                    'FIXTURE_ROOT': str(self.root), 'TEAMCITY_FC_PLAN': str(self.plan)}

    def run_shell(self, command, mock=True, check=True, env=None):
        prefix = 'source "$1"\n'
        if mock:
            prefix += '''teamcity_current_pr(){ [[ ${PR_FRESH:-true} == true ]]; }
curl(){ python3 "$FIXTURE_ROOT/status_mock.py" "$@"; }
'''
        result = subprocess.run(['bash', '-c', prefix + command, 'test', str(HELPER)],
                                env={**self.env, **(env or {})}, text=True, capture_output=True)
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def skipped_contexts(self):
        p = self.root / 'statuses.json'
        return [row['context'] for row in json.loads(p.read_text())] if p.exists() else []

    def test_unrelated_files_skip_without_submitting(self):
        self.run_shell('trigger_or_skip_build false 123 master ' + HEAD + ' feut')
        self.assertEqual(self.skipped_contexts(), ['TeamCity/feut'])
        self.assertFalse(self.plan.exists())

    def test_lane_approval_requires_an_open_pr_and_matching_file_snapshot(self):
        workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        step = workflow.split('    - name: Prepare FC lane selection\n', 1)[1].split('    - name:', 1)[0]
        script = textwrap.dedent(step.split('      run: |\n', 1)[1])
        (self.root / 'all_files').write_text('be/src/example.cpp\n')
        base_sha = 'c' * 40
        pr = {'state': 'open', 'draft': True, 'head': {'sha': HEAD}, 'base': {'ref': 'master', 'sha': base_sha}}
        for update, count, success in [({}, 1, True), ({'state': 'closed'}, 1, False),
                                       ({'head': {'sha': 'b' * 40}}, 1, False),
                                       ({'base': {'ref': 'branch-4.2', 'sha': base_sha}}, 1, False),
                                       ({'base': {'ref': 'master', 'sha': 'd' * 40}}, 1, False),
                                       ({}, 2, False), (None, 1, False)]:
            with self.subTest(update=update, count=count):
                plan = self.root / 'teamcity-fc-lanes'
                plan.unlink(missing_ok=True)
                env = {**self.env, 'PR': '123', 'HEAD_SHA': HEAD, 'BASE_SHA': base_sha, 'TARGET_BRANCH': 'master',
                       'FILE_COUNT': str(count), 'MOCK_PR': json.dumps({**pr, **update}) if update is not None else '',
                       'GITHUB_ENV': str(self.root / 'env'), 'RUNNER_TEMP': str(self.root)}
                result = subprocess.run(['bash', '-eo', 'pipefail', '-c', 'curl(){ echo "$MOCK_PR"; }\n' + script],
                                        cwd=self.root, env=env, text=True, capture_output=True)
                self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
                self.assertEqual(plan.exists(), success)

    def test_compile_skip_covers_downstream_checks(self):
        self.run_shell('trigger_or_skip_build false 123 master ' + HEAD + ' compile')
        self.assertEqual(self.skipped_contexts(), ['TeamCity/compile', 'TeamCity/p0', 'TeamCity/cloud_p0',
                                                  'TeamCity/vault_p0', 'TeamCity/external', 'TeamCity/nonconcurrent'])
        self.assertFalse(self.plan.exists())

    def test_failed_skip_and_stale_head_do_not_succeed(self):
        for env in [{'STATUS_FAIL': 'true'}, {'PR_FRESH': 'false'}]:
            result = self.run_shell('trigger_or_skip_build false 123 master ' + HEAD + ' feut', check=False, env=env)
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
        result = self.run_shell('for lane in "${!conment_to_context[@]}"; do echo "$lane ${conment_to_context[$lane]}"; done')
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
                                     'MOCK_GITHUB_RESPONSE': json.dumps({'head': {'sha': HEAD}, 'base': {'ref': branch, 'sha': 'c' * 40}, 'changed_files': 1, 'permission': permission})})
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result, output.read_text()

    def test_workflow_parses_exact_check_keywords(self):
        for lane in ['compile', 'feut', 'beut', 'cloudut', 'p0', 'cloud_p0', 'vault_p0',
                     'external', 'nonconcurrent', 'performance', 'buildall']:
            result, output = self.parse_comment('run ' + lane + ' 2')
            self.assertIn('COMMENT_TRIGGER_TYPE=' + lane + '\n', output)
            self.assertIn('COMMENT_REPEAT_TIMES=2\n', output)
            self.assertIn('base_sha=' + 'c' * 40 + '\n', output)
            self.assertIn('file_count=1\n', output)
        for command in ['run feut-extra', 'please run feut', 'run nonConcurrent', 'run p1', 'run cloud_p1', 'run check_coverage',
                        'run debugcompile', 'run debugall', 'run p0 21', 'run p0 0']:
            result, output = self.parse_comment(command, check=False)
            self.assertNotIn('COMMENT_TRIGGER_TYPE=', output)

    def test_workflow_plans_lanes_and_scopes_oidc_to_submission(self):
        workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        for title in ['Prepare FC lane selection', 'Collect selected FC lanes']:
            step = workflow.split('    - name: ' + title + '\n', 1)[1].split('      run: |', 1)[0]
            self.assertIn("if: steps.parse.outputs.comment_trigger == 'true'", step)
        gate, submit = workflow.split('  submit-fc:\n')
        self.assertNotIn('TEAMCITY_CURRENT_TOKEN', workflow)
        self.assertNotIn('id-token: write', gate)
        self.assertIn('id-token: write', submit)
        self.assertIn('needs: [check-comment-if-need-to-trigger-teamcity, approve-fc]', submit)
        for lane in ['compile', 'p0', 'feut']:
            self.run_shell('trigger_or_skip_build true 123 master ' + HEAD + ' ' + lane)
        self.run_shell('trigger_or_skip_build false 123 master ' + HEAD + ' cloudut')
        self.assertEqual(self.plan.read_text().splitlines(), ['compile', 'p0', 'feut'])
        self.assertEqual(self.skipped_contexts(), ['TeamCity/cloudut'])

    def test_missing_plan_stops_submission(self):
        result = self.run_shell('trigger_or_skip_build true 123 master ' + HEAD + ' p0',
                                env={'TEAMCITY_FC_PLAN': ''}, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing FC lane plan', result.stderr)

    def test_fc_submission_rejects_unexpected_url_before_requesting_oidc(self):
        mock = 'curl() { echo "unexpected curl call" >&2; return 1; }\n'
        for endpoint in ('', 'https://fc.example/trigger', FC_TRIGGER_URL + '/',
                         FC_TRIGGER_URL + '?source=ci', FC_TRIGGER_URL.replace('https:', 'http:')):
            with self.subTest(endpoint=endpoint):
                result = self.run_shell(mock + 'teamcity_fc_submit p0 123 ' + HEAD + ' master 1',
                                        mock=False, check=False, env={
                                            'TEAMCITY_FC_TRIGGER_URL': endpoint,
                                            'ACTIONS_ID_TOKEN_REQUEST_URL': 'https://oidc.example/token',
                                            'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'job-token',
                                        })
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('unexpected curl call', result.stderr)

    def test_fc_submission_validates_receipts_and_never_reposts(self):
        mock = '''curl(){
    local header
    read -r header
    if [[ "$*" == *'audience=urn:apache:doris:ci:teamcity-trigger'* ]]; then
        [[ "$header" == 'Authorization: Bearer job-token' ]] || return 1
        echo '{"value":"fixture-oidc"}'
    else
        [[ "${!#}" == "$TEAMCITY_FC_TRIGGER_URL" &&
           "$header" == 'Authorization: Bearer fixture-oidc' ]] || return 1
        while [[ $# -gt 0 && "$1" != --data-binary ]]; do shift; done
        echo "$2" >> "$FIXTURE_ROOT/fc-posts"
        echo "$FC_REPLY"
        echo "$FC_STATUS"
        return "$FC_EXIT"
    fi
}
'''
        for reply, status, exit_code, success in [({'build_ids': [17, 18]}, 200, 0, True),
                                                  ({'build_ids': [17]}, 200, 0, False),
                                                  ({'build_ids': [0, '18']}, 200, 0, False),
                                                  ({'build_ids': [17, 18]}, 202, 0, False),
                                                  ({'error': 'reconciliation_required'}, 409, 0, False),
                                                  ({}, 0, 28, False)]:
            posts = self.root / 'fc-posts'
            posts.unlink(missing_ok=True)
            env = {'TEAMCITY_FC_TRIGGER_URL': FC_TRIGGER_URL,
                   'ACTIONS_ID_TOKEN_REQUEST_URL': 'https://oidc.example/token',
                   'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'job-token',
                   'FC_REPLY': json.dumps(reply), 'FC_STATUS': str(status), 'FC_EXIT': str(exit_code)}
            result = self.run_shell(mock + 'teamcity_fc_submit p0 123 ' + HEAD + ' master 2',
                                    mock=False, check=False, env=env)
            self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
            self.assertEqual([json.loads(row) for row in posts.read_text().splitlines()],
                             [{'pr': 123, 'head_sha': HEAD, 'base_branch': 'master', 'pipeline': 'p0', 'repeat': 2}])

    def test_skip_buildall_authorization_is_preserved(self):
        _, output = self.parse_comment('skip buildall')
        self.assertIn('comment_skip=true', output)
        result, _ = self.parse_comment('skip buildall', branch='feature/example', check=False)
        self.assertNotEqual(result.returncode, 0)
        _, output = self.parse_comment('skip buildall', user_id='123456', check=False)
        self.assertNotIn('comment_skip=true', output)
        _, output = self.parse_comment('skip buildall', branch='branch-4.2', user_id='9208457')
        self.assertIn('comment_skip=true', output)

    def test_skip_buildall_statuses(self):
        workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        block = workflow.split('    - name: "Skip buildall"\n', 1)[1].split('\n  approve-fc:', 1)[0]
        script = textwrap.dedent(block.split('      run: |\n', 1)[1])
        script = script.replace('source ./regression-test/pipeline/common/teamcity-utils.sh', ':')
        script = script.replace('${{ steps.parse.outputs.COMMIT_ID_FROM_TRIGGER }}', HEAD)
        for branch in ['master', 'branch-4.2', 'branch-4.1']:
            (self.root / 'statuses.json').write_text('[]')
            self.run_shell(script, env={'TARGET_BRANCH': branch})
            expected = {'TeamCity/' + lane for lane in ['compile', 'feut', 'beut', 'cloudut', 'p0', 'cloud_p0', 'vault_p0', 'external', 'nonconcurrent']}
            if branch == 'master': expected.add('TeamCity/performance')
            self.assertEqual(set(self.skipped_contexts()), expected)
            self.assertFalse(self.plan.exists())

    def test_workflow_branch_and_permission_checks(self):
        for branch in ['master', 'branch-4.2', 'branch-4.0', 'feature/新版本']:
            for command in ['run performance', 'run buildall', 'run feut']:
                self.parse_comment(command, branch=branch)
            result, _ = self.parse_comment('run feut', branch=branch, permission='read', check=False)
            self.assertNotEqual(result.returncode, 0)

    def test_lane_selection_accepts_any_pr_target(self):
        for branch in ['branch-4.0', 'feature/新版本']:
            for lane in ['feut', 'performance']:
                self.run_shell('trigger_or_skip_build true 123 "$TARGET_BRANCH" ' + HEAD + ' ' + lane,
                               env={'TARGET_BRANCH': branch})
        self.assertEqual(self.plan.read_text().splitlines(), ['feut', 'performance'] * 2)

    def test_status_token_is_not_traced(self):
        result = self.run_shell('curl(){ return 0; }; set -x; skip_build "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" feut',
                                mock=False, env={'GITHUB_TOKEN': 'test-github-token'})
        self.assertNotIn('test-github-token', result.stdout + result.stderr)

    def test_workflow_dispatch_matrix(self):
        workflow = (HELPER.parents[3] / '.github/workflows/comment-to-trigger-teamcity.yml').read_text()
        conditions = dict(re.findall(r'    - name: "Trigger ([a-z_]+)"\n      if: \$\{\{ (.*?) \}\}', workflow))
        for branch in ['master', 'branch-4.2', 'branch-4.1']:
            for command in [*conditions, 'buildall']:
                selected = set()
                for lane, condition in conditions.items():
                    expression = condition.replace('steps.parse.outputs.comment_trigger', "'true'")
                    expression = expression.replace('steps.parse.outputs.COMMENT_TRIGGER_TYPE', repr(command))
                    expression = expression.replace('steps.parse.outputs.TARGET_BRANCH', repr(branch)).replace('&&', 'and')
                    if eval(expression, {'__builtins__': {}, 'fromJSON': json.loads,
                                         'contains': lambda values, value: value in values}):
                        selected.add(lane)
                expected = {'feut', 'beut', 'cloudut', 'compile', 'performance'} if command == 'buildall' else {command}
                self.assertEqual(selected, expected, (branch, command))

    def test_removed_lane_has_no_legacy_fallback(self):
        self.assertNotEqual(self.run_shell('trigger_or_skip_build true 123 master ' + HEAD + ' p1', check=False).returncode, 0)
        self.assertFalse(self.plan.exists())


if __name__ == '__main__':
    unittest.main()
