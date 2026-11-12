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

"""Validate the published Compile contract and pin every regression consumer.

Run on the TeamCity agent as the first trusted build step, using only the
build-scoped credentials supplied by TeamCity. Comments do not resolve artifacts.
Consumers verify package hashes again after download. The compiled base/merge are pinned from the manifest; a
later advance of the target branch does not invalidate the same PR head.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def consumer_parameters(build, raw_manifest, artifacts, pr, head, branch):
    require(build['buildTypeId'] == 'Doris_Compile' and build['state'] == 'finished'
            and build['status'] == 'SUCCESS' and not build.get('canceledInfo'),
            'Compile must be finished and successful')
    require(build['branchName'] == 'pull/' + str(pr)
            and [r['version'] for r in build['revisions']['revision']] == [head],
            'Compile PR/revision differs from the requested PR head')
    bid = str(build['id'])
    require(re.fullmatch(r'[1-9][0-9]*', bid) and build['number']
            and build['number'] != 'UNBOUND', 'Invalid Compile build identity')
    manifest = json.loads(raw_manifest)
    source, producer = manifest['source'], manifest['producer']
    require(manifest['schema'] == 1 and manifest['trust_class'] == 'pr-merge',
            'Unsupported Compile manifest')
    require(source['repository'] == 'apache/doris' and source['pr_number'] == int(pr)
            and source['pr_head_sha'] == head and source['target_branch'] == branch,
            'Compile manifest PR/head/target differs from the request')
    for key in ('pr_head_sha', 'target_head_sha', 'checkout_sha'):
        require(re.fullmatch(r'[0-9a-f]{40}', source[key]), 'Invalid source SHA: ' + key)
    require(str(producer['compiled_in_build']) == bid == str(producer['packaged_in_build']),
            'Compile and package producer differ')
    require(source['modified'] is False and manifest['frameworkSource']['modified'] is False
            and manifest['frameworkSource']['checkout_sha'] == source['checkout_sha'],
            'Compile source/framework was modified')
    require(manifest['unchangedTestTree']['unchanged'] is True,
            'Regression test tree was modified')
    proof = manifest['unchangedTestTree']['treeSha256']
    require(re.fullmatch(r'[0-9a-f]{64}', proof), 'Invalid test tree proof')
    require(isinstance(producer['image'], str) and producer['image'], 'Missing producer image')
    require(manifest['publication_state'] == 'uploaded-awaiting-teamcity-terminal-success'
            and manifest['consumer_gate'] == {'teamcity_status': 'SUCCESS',
                                             'pr_head_must_match': True, 'verify_sha256': True},
            'Compile packages were not published with consumer gates')
    expiry = datetime.datetime.fromisoformat(manifest['expires_at'].replace('Z', '+00:00'))
    require(expiry > datetime.datetime.now(datetime.timezone.utc), 'Compile packages have expired')
    packages = manifest['packages']
    require(set(packages) == {'binary.tar.gz', 'cases.tar.gz'}, 'Missing binary/case package')
    files = {f['name']: f for f in artifacts.get('file', [])}
    for name, package in packages.items():
        require(re.fullmatch(r'[0-9a-f]{64}', package['sha256'])
                and isinstance(package['bytes'], int) and 0 < package['bytes'] < 30 * 1024**3,
                'Invalid package digest/size: ' + name)
        require(name in files and files[name].get('size') == package['bytes'],
                'Missing or incomplete TeamCity artifact: ' + name)
    digest = hashlib.sha256(raw_manifest).hexdigest()
    binary = packages['binary.tar.gz']
    # Match the Compile fanout contract, including legacy aliases still consumed
    # by the five server-side runners. Never inherit stale configuration defaults.
    values = {
        'PR_NUMBER': str(pr), 'COMPILED_PR_HEAD': head,
        'CI_COMPILE_BUILD_ID': bid, 'CI_BINARY_SHA256': binary['sha256'],
        'CI_PREPARED_MANIFEST_SHA256': digest, 'CI_PREPARED_BUILD_NUMBER': build['number'],
        'COMPILE_BUILD_ID': bid, 'EXPECTED_COMPILE_BUILD_ID': bid,
        'COMPILE_MANIFEST_SHA256': digest, 'EXPECTED_COMPILE_MANIFEST_SHA256': digest,
        'EXPECTED_ORIGINAL_MANIFEST_SHA256': digest, 'ORIGINAL_MANIFEST_SHA256': digest,
        'COMPILE_BINARY_SHA256': binary['sha256'], 'EXPECTED_BINARY_SHA256': binary['sha256'],
        'EXPECTED_COMPILE_BINARY_SHA256': binary['sha256'], 'ORIGINAL_BINARY_SHA256': binary['sha256'],
        'COMPILE_BINARY_BYTES': str(binary['bytes']), 'EXPECTED_COMPILE_BINARY_BYTES': str(binary['bytes']),
        'EXPECTED_PR_HEAD': head, 'PR_HEAD_SHA': head,
        'EXPECTED_COMPILE_BASE': source['target_head_sha'], 'EXPECTED_PR_BASE_SHA': source['target_head_sha'],
        'EXPECTED_BASE_SHA': source['target_head_sha'], 'PR_BASE_SHA': source['target_head_sha'],
        'SOURCE_SHA': source['checkout_sha'], 'EXPECTED_SOURCE_SHA': source['checkout_sha'],
        'PR_MERGE_SHA': source['checkout_sha'],
        'TARGET_BRANCH': branch, 'PR_TARGET_BRANCH': branch, 'CI_BRANCH': branch,
        'PACKAGE_BUILD_ID': bid, 'PREPARED_BUILD_ID': bid, 'EXPECTED_PREPARED_BUILD_ID': bid,
        'PACKAGE_BUILD_NUMBER': build['number'], 'PACKAGE_MANIFEST_SHA256': digest,
        'PREPARED_MANIFEST_SHA256': digest, 'EXPECTED_MANIFEST_SHA256': digest,
        'EXPECTED_PRODUCER_IMAGE': producer['image'], 'TEST_TREE_PROOF_SHA256': proof,
        'COMPILE_VERIFIED_STATE': 'finished', 'COMPILE_VERIFIED_STATUS': 'SUCCESS',
        'PREPARED_VERIFIED_STATE': 'finished', 'PREPARED_VERIFIED_STATUS': 'SUCCESS',
    }
    return {'env.' + key: str(value) for key, value in values.items()}


DESTINATIONS = {'Doris_P0': 'packages', 'Doris_NonConcurrent': 'packages',
                'Doris_CloudP0': 'compile-artifacts', 'Doris_VaultP0': 'compile-artifacts',
                'Doris_External': 'compile-pair'}
BUILD_FIELDS = 'id,number,buildTypeId,state,status,branchName,canceledInfo,revisions(revision(version))'


def build_properties(path):
    # TeamCity writes generated system properties as key=value, with Java escapes.
    def unescape(match):
        value = match.group(1)
        return chr(int(value[1:], 16)) if value.startswith('u') else {
            't': '\t', 'r': '\r', 'n': '\n', 'f': '\f'}.get(value, value)
    result = {}
    for line in Path(path).read_text().splitlines():
        if line and not line.startswith(('#', '!')) and '=' in line:
            key, value = line.split('=', 1)
            result[key] = re.sub(r'\\(u[0-9a-fA-F]{4}|.)', unescape, value)
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Artifact redirect rejected')


class TeamCity:
    def __init__(self, server, properties):
        require(re.fullmatch(r'https://[A-Za-z0-9.-]+(:[0-9]+)?', server), 'TeamCity HTTPS URL required')
        self.server = server
        credentials = properties['teamcity.auth.userId'] + ':' + properties['teamcity.auth.password']
        self.headers = {'Authorization': 'Basic ' + base64.b64encode(credentials.encode()).decode(),
                        'Accept': 'application/json'}
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def open(self, path):
        require(path.startswith('builds') and not path.startswith('//'), 'Unexpected REST resource')
        return self.opener.open(urllib.request.Request(self.server + '/app/rest/' + path,
                                                       headers=self.headers), timeout=60)

    def read(self, path):
        with self.open(path) as response:
            raw = response.read(8 * 1024 * 1024 + 1)
        require(len(raw) <= 8 * 1024 * 1024, 'Oversized TeamCity response')
        return raw

    def get(self, path):
        return json.loads(self.read(path))

    def download(self, bid, name, destination, metadata):
        digest, size = hashlib.sha256(), 0
        with self.open('builds/id:' + bid + '/artifacts/content/regression/' +
                       urllib.parse.quote(name, safe='/')) as response, destination.open('xb') as output:
            while True:
                chunk = response.read(8 * 1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                require(size <= metadata['bytes'], 'Oversized artifact: ' + name)
                digest.update(chunk)
                output.write(chunk)
        require(size == metadata['bytes'] and digest.hexdigest() == metadata['sha256'],
                'Artifact size/SHA256 mismatch: ' + name)


def resolve(api, current, env):
    require(current['buildTypeId'] in DESTINATIONS, 'Not a regression consumer')
    match = re.fullmatch(r'pull/([1-9][0-9]*)', current['branchName'])
    require(match, 'Consumer must have a PR branch')
    pr = match.group(1)
    revisions = current['revisions']['revision']
    require(len(revisions) == 1 and re.fullmatch(r'[0-9a-f]{40}', revisions[0]['version']),
            'Consumer must have one exact PR revision')
    head = revisions[0]['version']
    require(env['PR_NUMBER'] == pr and env.get('CI_REQUESTED_PR_HEAD', head) == head,
            'Requested PR/commit differs from TeamCity revision')
    branch = env['CI_BRANCH']
    require(branch in {'master', 'branch-4.2', 'branch-4.1'}, 'Unsupported target branch')
    bid = env.get('CI_COMPILE_BUILD_ID', '')
    pinned = bool(bid and env.get('CI_PREPARED_BUILD_NUMBER', 'UNBOUND') != 'UNBOUND')
    if not pinned:
        require(not env.get('CI_FANOUT_KEY'), 'Compile fanout is missing its producer binding')
        path = ('builds?locator=buildType:(id:Doris_Compile),branch:(name:pull/' + pr +
                '),state:finished,status:SUCCESS,defaultFilter:false,count:100&fields=nextHref,build(' + BUILD_FIELDS + ')')
        seen = set()
        while True:
            require(path not in seen, 'Repeated Compile listing page')
            seen.add(path)
            page = api.get(path)
            candidates = [b for b in page.get('build', [])
                          if [r['version'] for r in b['revisions']['revision']] == [head]]
            if candidates:
                bid = str(candidates[0]['id'])
                break
            next_page = page.get('nextHref', '')
            require(next_page, 'No successful Compile for PR ' + pr + ' at ' + head + '; run compile first')
            require(next_page.startswith('/app/rest/builds?'), 'Invalid Compile listing page')
            path = next_page[len('/app/rest/'):]
    require(re.fullmatch(r'[1-9][0-9]*', bid), 'Invalid Compile build ID')
    build = api.get('builds/id:' + bid + '?fields=' + BUILD_FIELDS)
    raw = api.read('builds/id:' + bid + '/artifacts/content/regression/manifest.json')
    files = api.get('builds/id:' + bid + '/artifacts/children/regression?fields=file(name,size)')
    params = consumer_parameters(build, raw, files, pr, head, branch)
    if pinned:
        require(env['CI_PREPARED_BUILD_NUMBER'] == build['number'], 'Pinned Compile number differs')
    if env.get('CI_FANOUT_KEY'):
        for key, value in params.items():
            require(env.get(key[4:]) == value, 'Compile fanout binding differs: ' + key)
    return bid, raw, params


def download_pair(api, bid, raw, destination, build_id):
    manifest = json.loads(raw)
    stage = destination.with_name('.' + destination.name + '-' + build_id)
    require(not stage.exists() and not destination.is_symlink(), 'Artifact staging already exists or is a link')
    stage.mkdir(mode=0o700)
    try:
        files = {**manifest['packages'], **manifest.get('tools', {})}
        for name, metadata in files.items():
            path = Path(name)
            require(not path.is_absolute() and '..' not in path.parts and path.parts,
                    'Unsafe artifact path')
            require(name in manifest['packages'] or name.startswith('tools/'), 'Unexpected artifact path')
            require(re.fullmatch(r'[0-9a-f]{64}', metadata['sha256']) and 0 < metadata['bytes'] < 30 * 1024**3,
                    'Invalid artifact digest/size')
            target = stage / path
            target.parent.mkdir(parents=True, exist_ok=True)
            print('Downloading Compile ' + bid + ': ' + name, flush=True)
            api.download(bid, name, target, metadata)
        (stage / 'manifest.json').write_bytes(raw)
        (stage / 'manifest.sha256').write_text(hashlib.sha256(raw).hexdigest() + '  manifest.json\n')
        # The same dedicated directory was previously cleaned by the native
        # artifact dependency. Never remove a checkout or an arbitrary path.
        require(destination.name in set(DESTINATIONS.values()) and destination.parent == Path.cwd(),
                'Unexpected artifact destination')
        if destination.exists():
            shutil.rmtree(destination)
        stage.rename(destination)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def escape(value):
    for source, replacement in [('|', '||'), ("'", "|'"), ('\n', '|n'), ('\r', '|r'), ('[', '|['), (']', '|]')]:
        value = value.replace(source, replacement)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', required=True)
    parser.add_argument('--environment', required=True, help='Private export file for this build step')
    args = parser.parse_args()
    require(os.getuid() != 0, 'Artifact preflight must run as the ordinary agent user')
    properties = build_properties(os.environ['TEAMCITY_BUILD_PROPERTIES_FILE'])
    build_id = properties['teamcity.build.id']
    require(re.fullmatch(r'[1-9][0-9]*', build_id), 'Invalid consumer build ID')
    api = TeamCity(args.server, properties)
    current = api.get('builds/id:' + build_id + '?fields=' + BUILD_FIELDS)
    bid, raw, params = resolve(api, current, os.environ)
    print('Matched Compile ' + bid + ' (' + params['env.CI_PREPARED_BUILD_NUMBER'] + ') for ' +
          current['branchName'] + ' at ' + params['env.PR_HEAD_SHA'], flush=True)
    download_pair(api, bid, raw, Path.cwd() / DESTINATIONS[current['buildTypeId']], build_id)
    # Publish only after the entire package pair and tools verify. Subsequent
    # TeamCity steps receive these parameters; credentials are never exported.
    Path(args.environment).write_text(''.join('export ' + key[4:] + '=' + shlex.quote(value) + '\n'
                                              for key, value in params.items()))
    Path(args.environment).chmod(0o600)
    for key, value in params.items():
        print("##teamcity[setParameter name='" + escape(key) + "' value='" + escape(value) + "']")
    print('Compile artifact preflight passed: producer=' + bid, flush=True)


if __name__ == '__main__':
    try:
        main()
    except (KeyError, TypeError, ValueError, OSError) as error:
        message = 'Compile artifact preflight failed: ' + str(error)
        print("##teamcity[buildProblem description='" + escape(message) + "']", flush=True)
        sys.exit(1)
