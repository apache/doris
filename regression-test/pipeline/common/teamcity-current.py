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

"""Route reviewed PR comments to the current TeamCity without legacy fallback."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

REPOSITORY = 'apache/doris'
BRANCHES = {'master', 'branch-4.2', 'branch-4.1'}
VCS_ROOT = 'DorisMigrationValidation_ApacheDoris'
ENTRIES = {'compile': 'Doris_Compile', 'feut': 'Doris_FEUT',
           'beut': 'Doris_BEUT', 'cloudut': 'Doris_CloudUT'}
REGRESSIONS = {'p0': 'Doris_P0', 'cloud_p0': 'Doris_CloudP0',
               'vault_p0': 'Doris_VaultP0', 'external': 'Doris_External',
               'nonConcurrent': 'Doris_NonConcurrent'}
PERFORMANCE_COMPILE = 'Doris_Performance_Compile'
PERFORMANCE = 'Doris_Performance'
CONTEXTS = ['TeamCity/' + name for name in
            ['compile', 'feut', 'beut', 'cloudut', 'p0', 'cloud_p0',
             'vault_p0', 'external', 'nonconcurrent', 'performance']]
FIELDS = ('id,number,buildTypeId,branchName,state,status,canceledInfo,'
          'revisions(revision(version)),properties(property(name,value))')
SHA = re.compile(r'[0-9a-f]{40}')
DIGEST = re.compile(r'[0-9a-f]{64}')
COMMAND = re.compile(
    r'(?<!\w)(run|skip)\s+(buildall|compile|feut|beut|cloudut|p0|p1|'
    r'cloud_p0|cloud_p1|vault_p0|external|nonConcurrent|performance|check_coverage)'
    r'(?!\w)(?:\s+([0-9]+))?')


class Error(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise Error(message)


def parse_command(body):
    matches = list(COMMAND.finditer(body))
    require(len(matches) == 1, 'Use one TeamCity command per comment.')
    action, pipeline, count = matches[0].groups()
    count = int(count or '1')
    require(1 <= count <= 20, 'Repeat count must be between 1 and 20.')
    require(action == 'run' or (pipeline in {'buildall', 'check_coverage'} and count == 1),
            'Only skip buildall and skip check_coverage are supported.')
    return action, pipeline, count


def properties(build):
    return {item['name']: item.get('value', '')
            for item in build.get('properties', {}).get('property', [])}


def revision_matches(build, pr):
    revisions = build.get('revisions', {}).get('revision', [])
    return (build.get('branchName') == 'pull/' + str(pr['number'])
            and len(revisions) == 1 and revisions[0]['version'] == pr['head']['sha'])


def terminal_success(build, build_type):
    return (build.get('buildTypeId') == build_type and build.get('state') == 'finished'
            and build.get('status') == 'SUCCESS' and not build.get('canceledInfo'))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise Error('Authenticated redirects are not accepted.')


class API:
    def __init__(self, base, token):
        parsed = urllib.parse.urlsplit(base)
        require(parsed.scheme == 'https' and parsed.hostname and not parsed.username
                and not parsed.password and not parsed.query and not parsed.fragment,
                'Configure an HTTPS TeamCity URL without credentials or query parameters.')
        require(token, 'The required API token is not configured.')
        self.base = base.rstrip('/') + '/'
        self.headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/json',
                        'Content-Type': 'application/json', 'User-Agent': 'doris-ci-comments'}
        self.client = urllib.request.build_opener(NoRedirect())

    def request(self, path, method='GET', data=None, raw=False):
        require(not path.startswith(('/', 'http:','https:')), 'Only relative API paths are accepted.')
        req = urllib.request.Request(self.base + path, headers=self.headers, method=method,
                                     data=None if data is None else json.dumps(data).encode())
        try:
            with self.client.open(req, timeout=45) as response:
                body = response.read(8 * 1024 * 1024 + 1)
            require(len(body) <= 8 * 1024 * 1024, 'API response exceeds the size limit.')
            return body if raw else (json.loads(body) if body else {})
        except urllib.error.HTTPError as error:
            raise Error(method + ' request failed: HTTP ' + str(error.code)) from None
        except (OSError, TimeoutError):
            # In particular, never blindly retry a POST whose response was lost.
            raise Error(method + ' request did not complete. Re-run this workflow to reconcile '
                        'the comment request before submitting another build.') from None


class GitHub(API):
    def __init__(self, token):
        super().__init__('https://api.github.com', token)

    def pr(self, number):
        return self.request('repos/' + REPOSITORY + '/pulls/' + str(number))

    def authorize(self, event, action, branch, pipeline=None):
        user = event['comment']['user']
        if action == 'skip':
            allowed = {27881198, 2899462, 37901441}
            if pipeline == 'check_coverage':
                allowed.update({9208457, 10553413, 1736049, 64513324, 2274896, 98214048,
                                19986811, 48427519, 6919662, 101034200})
            if branch in {'branch-4.1', 'branch-4.2'}:
                allowed.add(9208457)
            require(user['id'] in allowed, 'This account is not authorized to skip these checks.')
            return
        login = urllib.parse.quote(user['login'], safe='')
        permission = self.request('repos/' + REPOSITORY + '/collaborators/' + login + '/permission')
        require(permission.get('permission') in {'write', 'maintain', 'admin'},
                'Repository write access is required to trigger CI from comments.')


class TeamCity(API):
    def __init__(self, base, token):
        super().__init__(base.rstrip('/') + '/app/rest', token)
        self.web_base = base.rstrip('/')
        self.instance = None

    def builds(self, build_type, pr=None):
        locator = 'buildType:(id:' + build_type + '),defaultFilter:false,count:100'
        if pr:
            locator += ',branch:(name:pull/' + str(pr['number']) + ')'
        rows = self.request('builds?locator=' + locator + '&fields=build(' + FIELDS + ')').get('build', [])
        queued = self.request('buildQueue?locator=buildType:(id:' + build_type
                              + ')&fields=build(' + FIELDS + ')').get('build', [])
        return list({row['id']: row for row in rows + queued}.values())

    def artifact(self, bid, path):
        return self.request('builds/id:' + str(bid) + '/artifacts/content/' + path, raw=True)

    def resolve_instance(self):
        if self.instance is None:
            rows = self.request('vcs-root-instances?fields=vcs-root-instance(id,vcs-root-id)')
            matches = [row for row in rows.get('vcs-root-instance', []) if row['vcs-root-id'] == VCS_ROOT]
            require(len(matches) == 1 and str(matches[0]['id']).isdigit(), 'The Doris VCS root is ambiguous.')
            self.instance = str(matches[0]['id'])
        return self.instance

    def check_source_root(self, build_type):
        config = self.request('buildTypes/id:' + build_type + '/vcs-root-entries')
        roots = config.get('vcs-root-entry', [])
        require(len(roots) == 1 and roots[0]['vcs-root']['id'] == VCS_ROOT,
                build_type + ' is not configured for general PR builds. No build was submitted.')

    def enqueue(self, build_type, pr, params, key, dependencies=None):
        require(build_type in {*ENTRIES.values(), *REGRESSIONS.values(), PERFORMANCE, PERFORMANCE_COMPILE},
                'Build configuration is not allowlisted.')
        self.check_source_root(build_type)
        params = {**params, 'env.CI_COMMENT_REQUEST_KEY': key}
        body = {'buildType': {'id': build_type}, 'branchName': 'pull/' + str(pr['number']),
                'revisions': {'failOnMissingRevisions': True, 'revision': [{
                    'version': pr['head']['sha'], 'vcsBranchName': 'refs/pull/' + str(pr['number']) + '/head',
                    'vcs-root-instance': {'id': self.resolve_instance()}}]},
                'properties': {'property': [{'name': k, 'value': v} for k, v in sorted(params.items())]},
                'triggeringOptions': {'queueAtTop': False, 'rebuildAllDependencies': False}}
        if dependencies is not None:
            body['custom-artifact-dependencies'] = {'artifact-dependency': dependencies}
        # Request keys survive uncertain HTTP responses and GitHub workflow retries.
        print('Submitting ' + build_type + ' request=' + key, flush=True)
        queued = self.request('buildQueue', 'POST', body)
        actual = self.request('builds/id:' + str(queued['id']) + '?fields=' + FIELDS)
        require(revision_matches(actual, pr) and all(properties(actual).get(k) == v for k, v in params.items()),
                'TeamCity did not retain the requested PR revision or parameters.')
        return actual


def validate_manifest(build, raw, pr, performance=False):
    build_type = PERFORMANCE_COMPILE if performance else ENTRIES['compile']
    require(terminal_success(build, build_type) and revision_matches(build, pr),
            'Only a successful terminal build of this exact PR revision may supply artifacts.')
    manifest = json.loads(raw)
    source, producer = manifest['source'], manifest['producer']
    expected = {'repository': REPOSITORY, 'pr_number': pr['number'],
                'pr_head_sha': pr['head']['sha'], 'target_branch': pr['base']['ref']}
    require(all(source.get(k) == v for k, v in expected.items()), 'Compile artifact source differs from the PR.')
    require(all(SHA.fullmatch(source[k]) for k in ['pr_head_sha', 'target_head_sha', 'checkout_sha']),
            'Compile artifact has an invalid source revision.')
    require(str(producer['compiled_in_build']) == str(producer['packaged_in_build']) == str(build['id']),
            'Artifact producer differs from the selected build.')
    require(manifest['trust_class'] == ('pr-merge-performance-release' if performance else 'pr-merge'),
            'Artifact has the wrong build flavor.')
    if performance:
        require(producer['build_type'] == PERFORMANCE_COMPILE and manifest['flavor']['status'] == 'PASS',
                'Performance requires a verified Release artifact.')
    else:
        expiry = datetime.datetime.fromisoformat(manifest['expires_at'].replace('Z', '+00:00'))
        require(expiry > datetime.datetime.now(datetime.timezone.utc), 'Compile artifact has expired.')
        require(source.get('modified') is False and manifest['frameworkSource'].get('modified') is False
                and not manifest.get('patches') and not manifest.get('frameworkPatched'),
                'Modified source or framework artifacts cannot be reused.')
        require(set(manifest['packages']) == {'binary.tar.gz', 'cases.tar.gz'} and manifest['tools']
                and DIGEST.fullmatch(manifest['unchangedTestTree']['treeSha256']), 'Incomplete regression inputs.')
    for package in manifest['packages'].values():
        require(DIGEST.fullmatch(package['sha256']) and 0 < package['bytes'] < 30 * 1024**3,
                'Invalid package size or digest.')
    return manifest, hashlib.sha256(raw).hexdigest()


def consumer_parameters(manifest, digest, build):
    source = manifest['source']; binary = manifest['packages']['binary.tar.gz']; bid = str(build['id'])
    values = {'PR_NUMBER': str(source['pr_number']), 'COMPILED_PR_HEAD': source['pr_head_sha'],
              'CI_COMPILE_BUILD_ID': bid, 'CI_BINARY_SHA256': binary['sha256'],
              'CI_PREPARED_MANIFEST_SHA256': digest, 'CI_PREPARED_BUILD_NUMBER': build['number'],
              'EXPECTED_PRODUCER_IMAGE': manifest['producer']['image'],
              'TEST_TREE_PROOF_SHA256': manifest['unchangedTestTree']['treeSha256'], 'CI_FANOUT_KEY': ''}
    aliases = [
        (bid, ['COMPILE_BUILD_ID', 'EXPECTED_COMPILE_BUILD_ID', 'PACKAGE_BUILD_ID',
               'PREPARED_BUILD_ID', 'EXPECTED_PREPARED_BUILD_ID']),
        (digest, ['COMPILE_MANIFEST_SHA256', 'EXPECTED_COMPILE_MANIFEST_SHA256', 'EXPECTED_ORIGINAL_MANIFEST_SHA256',
                  'ORIGINAL_MANIFEST_SHA256', 'PACKAGE_MANIFEST_SHA256', 'PREPARED_MANIFEST_SHA256', 'EXPECTED_MANIFEST_SHA256']),
        (binary['sha256'], ['COMPILE_BINARY_SHA256', 'EXPECTED_BINARY_SHA256', 'EXPECTED_COMPILE_BINARY_SHA256', 'ORIGINAL_BINARY_SHA256']),
        (str(binary['bytes']), ['COMPILE_BINARY_BYTES', 'EXPECTED_COMPILE_BINARY_BYTES']),
        (source['pr_head_sha'], ['EXPECTED_PR_HEAD', 'PR_HEAD_SHA']),
        (source['target_head_sha'], ['EXPECTED_COMPILE_BASE', 'EXPECTED_PR_BASE_SHA', 'EXPECTED_BASE_SHA', 'PR_BASE_SHA']),
        (source['checkout_sha'], ['SOURCE_SHA', 'EXPECTED_SOURCE_SHA', 'PR_MERGE_SHA']),
        (source['target_branch'], ['TARGET_BRANCH', 'PR_TARGET_BRANCH', 'CI_BRANCH']),
        (build['number'], ['PACKAGE_BUILD_NUMBER']),
        ('finished', ['COMPILE_VERIFIED_STATE', 'PREPARED_VERIFIED_STATE']),
        ('SUCCESS', ['COMPILE_VERIFIED_STATUS', 'PREPARED_VERIFIED_STATUS']),
    ]
    for value, names in aliases:
        values.update({name: value for name in names})
    return {'env.' + name: str(value) for name, value in values.items()}


class Trigger:
    def __init__(self, github, teamcity, event, pr):
        self.github, self.tc, self.event, self.pr = github, teamcity, event, pr
        comment = event['comment']
        revision = hashlib.sha256((comment['body'] + comment['updated_at']).encode()).hexdigest()[:12]
        self.key = 'github-comment:' + str(comment['id']) + ':' + revision + ':' + pr['head']['sha']

    def fresh(self):
        current = self.github.pr(self.pr['number'])
        require(current['state'] == 'open' and not current['draft']
                and current['head']['sha'] == self.pr['head']['sha']
                and current['base']['ref'] == self.pr['base']['ref'],
                'PR closed, became a draft, or changed revision/target. No further builds were submitted.')

    def submit(self, build_type, params, count=1, dependencies=None, covers=None):
        rows = [row for row in self.tc.builds(build_type, self.pr) if revision_matches(row, self.pr)]
        used = set()
        reserved = {row['id'] for row in rows
                    if properties(row).get('env.CI_COMMENT_REQUEST_KEY', '').startswith(
                        self.key + ':' + build_type + ':')}
        selected = []
        for ordinal in range(count):
            key = self.key + ':' + build_type + ':' + str(ordinal + 1)
            exact = [row for row in rows if properties(row).get('env.CI_COMMENT_REQUEST_KEY') == key]
            require(len(exact) <= 1, 'Duplicate comment request keys require reconciliation.')
            active = [row for row in rows if row['id'] not in used and row['id'] not in reserved
                      and row.get('state') in {'queued', 'running'}
                      and row.get('status') != 'FAILURE' and not row.get('canceledInfo')
                      and (covers is None or covers(row))]
            if exact or active:
                build = (exact or active)[0]
                print('Reusing ' + build_type + ' build ' + str(build['id']))
            else:
                self.fresh()
                build = self.tc.enqueue(build_type, self.pr, params, key, dependencies)
                rows.append(build)
            used.add(build['id']); selected.append(build)
            summary('- ' + build_type + ': [build ' + str(build['id']) + ']('
                    + self.tc.web_base + '/buildConfiguration/' + build_type + '/' + str(build['id']) + ')')
        return selected

    def compile(self, targets, count=1):
        requested = set(targets)
        def covers(row):
            params = properties(row)
            return (params.get('is_debug', 'false') == 'false'
                    and requested <= set(params.get('regression_targets', ','.join(REGRESSIONS)).split(',')))
        params = {'env.PR_NUMBER': str(self.pr['number']), 'env.CI_BRANCH': self.pr['base']['ref'],
                  'is_debug': 'false', 'debug_real_steps': '', 'external_debug_hold_seconds': '0',
                  'regression_targets': ','.join(targets), 'env.CI_COMPILE_REQUEST_KEY': self.key}
        return self.submit(ENTRIES['compile'], params, count, covers=covers)

    def producer(self, performance=False):
        build_type = PERFORMANCE_COMPILE if performance else ENTRIES['compile']
        path = 'performance-release-packages/manifest.json' if performance else 'regression/manifest.json'
        candidates = sorted(self.tc.builds(build_type, self.pr), key=lambda row: row['id'], reverse=True)
        for build in candidates:
            if not (terminal_success(build, build_type) and revision_matches(build, self.pr)):
                continue
            raw = self.tc.artifact(build['id'], path)
            if not performance:
                expiry = datetime.datetime.fromisoformat(json.loads(raw)['expires_at'].replace('Z', '+00:00'))
                if expiry <= datetime.datetime.now(datetime.timezone.utc):
                    continue
            manifest, digest = validate_manifest(build, raw, self.pr, performance)
            if performance:
                proof = self.tc.artifact(build['id'], 'performance-release-evidence/performance-flavor-proof.json')
                require(hashlib.sha256(proof).hexdigest() == manifest['proof']['sha256']
                        and json.loads(proof) == manifest['flavor'], 'Release flavor proof differs from the manifest.')
                cleanup = json.loads(self.tc.artifact(build['id'], 'performance-cleanup/cleanup-receipt.json'))
                require(cleanup['complete'] and cleanup['cleanup_exit'] == 0, 'Release producer cleanup is incomplete.')
            else:
                identity = json.loads(self.tc.artifact(build['id'], 'identity/identity.json'))
                source = manifest['source']
                expected = {'build_id': str(build['id']), 'repository': REPOSITORY,
                            'pr_number': source['pr_number'], 'pr_head_sha': source['pr_head_sha'],
                            'merge_sha': source['checkout_sha'], 'base_sha': source['target_head_sha'],
                            'branch': source['target_branch']}
                require(all(identity.get(k) == v for k, v in expected.items()),
                        'Checkout evidence differs from the manifest.')
                binary = json.loads(self.tc.artifact(build['id'], 'validation/binary.json'))
                require(binary['sha256'] == manifest['packages']['binary.tar.gz']['sha256']
                        and binary['source_sha'] == source['checkout_sha']
                        and binary['build_id'] == str(build['id'])
                        and binary['bytes'] == manifest['packages']['binary.tar.gz']['bytes']
                        and binary['image'] == manifest['producer']['image'],
                        'Binary evidence differs from the manifest.')
            return build, manifest, digest
        return None

    def regression(self, pipeline, count):
        producer = self.producer()
        if producer is None:
            print('No verified artifact for this PR revision; Compile will trigger ' + pipeline + '.')
            return self.compile([pipeline], count)
        build, manifest, digest = producer
        target = REGRESSIONS[pipeline]
        # Keep each lane's native artifact layout, but pin its dependency to this exact producer.
        response = self.tc.request('buildTypes/id:' + target + '/artifact-dependencies')
        dependencies = response.get('artifact-dependency', [])
        require(len(dependencies) == 1 and dependencies[0]['source-buildType']['id'] == ENTRIES['compile'],
                'The regression artifact dependency is not bound to Compile.')
        dependency = dependencies[0]
        dep_params = properties(dependency)
        dep_params.update(revisionName='buildNumber', revisionValue=build['number'])
        custom = {'type': 'artifact_dependency', 'source-buildType': {'id': ENTRIES['compile']},
                  'properties': {'property': [{'name': k, 'value': v} for k, v in dep_params.items()]}}
        params = consumer_parameters(manifest, digest, build)
        return self.submit(target, params, count, [custom],
                           covers=lambda row: all(properties(row).get(k) == v for k, v in params.items()
                                                  if k != 'env.CI_FANOUT_KEY'))

    def baseline(self):
        build_type = 'Doris_Performance_BaselineCompatibility'
        candidates = sorted(self.tc.builds(build_type), key=lambda row: row['id'], reverse=True)
        for build in candidates:
            if not terminal_success(build, build_type):
                continue
            params = properties(build)
            digest, uuid = params.get('env.PERF_SEED_MANIFEST_SHA256', ''), params.get('env.PERF_DATA_UUID', '')
            require(DIGEST.fullmatch(digest) and re.fullmatch(r'[0-9a-f-]{36}', uuid), 'Baseline binding is missing.')
            prefix = 'performance-baseline-evidence/'
            before = json.loads(self.tc.artifact(build['id'], prefix + 'baseline-before.json'))
            after = json.loads(self.tc.artifact(build['id'], prefix + 'baseline-after.json'))
            rows = json.loads(self.tc.artifact(build['id'], prefix + 'actual-table-rows.json'))
            cleanup = json.loads(self.tc.artifact(build['id'], prefix + 'cleanup.json'))
            require(all(item['manifest_sha256'] == digest and item['baseline_immutable_to_agent']
                        for item in [before, after]) and before['files_verified'] > 0
                    and before['files_verified'] == after['files_verified'], 'Baseline verification is incomplete.')
            require(rows['state'] == 'ALL_THREE_DATASETS_ACTUAL_ROWS_MATCH' and len(rows['tables']) == 33
                    and all(row['actual'] == row['expected'] for row in rows['tables'].values())
                    and cleanup['complete'] and cleanup['cleanup_exit'] == 0, 'Baseline data or cleanup is incomplete.')
            return {'env.PERF_SEED_ROOT': '/data/performance-baseline/master',
                    'env.PERF_SEED_MANIFEST_SHA256': digest, 'env.PERF_DATA_UUID': uuid}
        raise Error('No successful Performance baseline compatibility build is available.')

    def performance(self, count):
        require(self.pr['base']['ref'] == 'master', 'The current Performance baseline supports master only.')
        self.tc.check_source_root(PERFORMANCE_COMPILE)
        self.tc.check_source_root(PERFORMANCE)
        baseline = self.baseline()
        producer = self.producer(performance=True)
        if producer is None:
            params = {'env.PR_NUMBER': str(self.pr['number']), 'env.CI_BRANCH': 'master'}
            build = self.submit(PERFORMANCE_COMPILE, params)[0]
            deadline = time.monotonic() + 180 * 60
            while build.get('state') != 'finished':
                self.fresh()
                require(time.monotonic() < deadline, 'Release compile is still running; its build is preserved.')
                print('Waiting for terminal Release Compile ' + str(build['id']), flush=True)
                time.sleep(20)
                build = self.tc.request('builds/id:' + str(build['id']) + '?fields=' + FIELDS)
            require(terminal_success(build, PERFORMANCE_COMPILE), 'Release Compile did not succeed; no benchmark queued.')
            producer = self.producer(performance=True)
            require(producer is not None, 'Successful Release artifact was not found.')
        build, manifest, digest = producer
        params = {**baseline, 'env.PERF_INPUT_MODE': 'native-teamcity-artifacts',
                  'env.PERF_COMPILED_BUILD_ID': str(build['id']), 'env.PERF_PRODUCER_BUILD_ID': str(build['id']),
                  'env.PERF_PACKAGE_TERMINAL_SUCCESS': 'true', 'env.PERF_MANIFEST_SHA256': digest,
                  'env.PERF_ORIGINAL_BINARY_SHA256': manifest['packages']['binary.tar.gz']['sha256'],
                  'env.PERF_BASE_SHA': manifest['source']['target_head_sha'],
                  'env.PERF_MERGE_SHA': manifest['source']['checkout_sha']}
        dep_params = {'cleanDestinationDirectory': 'true', 'revisionName': 'buildNumber',
                      'revisionValue': build['number'],
                      'pathRules': 'performance-release-packages/manifest.json => performance-input-%teamcity.build.id%\n'
                                   'performance-release-packages/binary.tar.gz => performance-input-%teamcity.build.id%'}
        dependency = {'type': 'artifact_dependency', 'source-buildType': {'id': PERFORMANCE_COMPILE},
                      'properties': {'property': [{'name': k, 'value': v} for k, v in dep_params.items()]}}
        return self.submit(PERFORMANCE, params, count, [dependency],
                           covers=lambda row: all(properties(row).get(k) == v for k, v in params.items()))


def output(name, value):
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write(name + '=' + str(value) + '\n')


def summary(message):
    print(message)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
            stream.write(message + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', choices=['route', 'standard', 'performance'], default='standard')
    parser.add_argument('--event', default=os.environ.get('GITHUB_EVENT_PATH'))
    args = parser.parse_args()
    require(args.event, 'The GitHub event file is required.')
    event = json.loads(Path(args.event).read_text())
    require(event['repository']['full_name'] == REPOSITORY and event['issue'].get('pull_request'),
            'Only apache/doris PR comments are accepted.')
    github = GitHub(os.environ.get('GITHUB_TOKEN', ''))
    pr = github.pr(int(event['issue']['number']))
    if args.phase == 'route':
        output('route', 'current' if pr['base']['ref'] in BRANCHES else 'legacy')
        return
    action, pipeline, count = parse_command(event['comment']['body'])
    require(pr['state'] == 'open' and not pr['draft'] and pr['base']['ref'] in BRANCHES,
            'The PR must be open, non-draft, and target a migrated branch.')
    require(SHA.fullmatch(pr['head']['sha']), 'Invalid PR revision.')
    expected_head = os.environ.get('EXPECTED_PR_HEAD')
    require(not expected_head or expected_head == pr['head']['sha'], 'PR head changed after the standard trigger.')
    github.authorize(event, action, pr['base']['ref'], pipeline)
    if action == 'skip':
        if pipeline == 'check_coverage':
            summary('The current pipeline has no separate coverage gate to skip.')
            return
        for context in CONTEXTS:
            github.request('repos/' + REPOSITORY + '/statuses/' + pr['head']['sha'], 'POST',
                           {'state': 'success', 'description': 'Skipped by authorized PR comment', 'context': context})
        summary('Current TeamCity checks skipped by the existing authorized account policy. Builds were not canceled.')
        return
    require(pipeline in {*ENTRIES, *REGRESSIONS, 'buildall', 'performance'},
            'This command has no current pipeline. No legacy pipeline was triggered.')
    tc = TeamCity(os.environ.get('TEAMCITY_CURRENT_URL', ''), os.environ.get('TEAMCITY_CURRENT_TOKEN', ''))
    trigger = Trigger(github, tc, event, pr)
    if args.phase == 'performance':
        require(pipeline in {'buildall', 'performance'}, 'This event does not request Performance.')
        trigger.performance(count)
        return
    output('head', pr['head']['sha'])
    output('performance_requested', str(pipeline in {'buildall', 'performance'} and pr['base']['ref'] == 'master').lower())
    if pipeline == 'performance':
        require(pr['base']['ref'] == 'master', 'Performance is available on master only and remains non-required.')
    elif pipeline == 'buildall':
        trigger.compile(list(REGRESSIONS), count)
        for lane in ['feut', 'beut', 'cloudut']:
            trigger.submit(ENTRIES[lane], {'env.PR_NUMBER': str(pr['number']), 'env.CI_BRANCH': pr['base']['ref']}, count)
    elif pipeline == 'compile':
        trigger.compile(list(REGRESSIONS), count)
    elif pipeline in REGRESSIONS:
        trigger.regression(pipeline, count)
    else:
        trigger.submit(ENTRIES[pipeline], {'env.PR_NUMBER': str(pr['number']), 'env.CI_BRANCH': pr['base']['ref']}, count)


if __name__ == '__main__':
    try:
        main()
    except (Error, KeyError, ValueError) as error:
        # Do not print HTTP bodies, authorization headers, or runtime parameter dictionaries.
        print('TeamCity comment trigger: ' + str(error), file=sys.stderr)
        sys.exit(1)

