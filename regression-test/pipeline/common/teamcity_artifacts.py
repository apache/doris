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

"""Bind native TeamCity artifact dependencies to the existing regression runners.

TeamCity selects and downloads the Compile artifacts. This first trusted step
checks their PR/revision and producer status, then fills the legacy runner aliases.
Existing preparation steps verify package checksums and the full manifest contract.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.request


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def consumer_parameters(build, raw_manifest, pr, head, branch, env):
    require(build['buildTypeId'] == 'Doris_Compile' and build['state'] == 'finished'
            and build['status'] == 'SUCCESS' and not build.get('canceledInfo'),
            'Compile must be finished and successful')
    require(build['branchName'] == 'pull/' + str(pr)
            and [r['version'] for r in build['revisions']['revision']] == [head],
            'No Compile package for the requested PR/commit; run compile first')
    manifest = json.loads(raw_manifest)
    source, producer = manifest['source'], manifest['producer']
    bid = str(build['id'])
    require(source['repository'] == 'apache/doris' and source['pr_number'] == int(pr)
            and source['pr_head_sha'] == head and source['target_branch'] == branch,
            'Compile manifest PR/head/target differs from the request')
    require(str(producer['compiled_in_build']) == bid == str(producer['packaged_in_build']),
            'Downloaded manifest differs from the native artifact dependency')
    require(env.get('CI_REQUESTED_PR_HEAD', head) == head,
            'Requested commit differs from TeamCity revision')
    proof = manifest['unchangedTestTree']['treeSha256']
    packages = manifest['packages']
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
    if env.get('CI_FANOUT_KEY'):
        for key, value in values.items():
            require(env.get(key) == str(value), 'Compile fanout binding differs: ' + key)
    return {'env.' + key: str(value) for key, value in values.items()}


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


def escape(value):
    for source, replacement in [('|', '||'), ("'", "|'"), ('\n', '|n'), ('\r', '|r'), ('[', '|['), (']', '|]')]:
        value = value.replace(source, replacement)
    return value



class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Unexpected TeamCity redirect')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', required=True)
    parser.add_argument('--manifest', required=True)
    args = parser.parse_args()
    require(os.getuid() != 0, 'Artifact preflight must run as the ordinary agent user')
    require(re.fullmatch(r'https://[A-Za-z0-9.-]+(:[0-9]+)?', args.server), 'TeamCity HTTPS URL required')
    raw = Path(args.manifest).read_bytes()
    properties = build_properties(os.environ['TEAMCITY_BUILD_PROPERTIES_FILE'])
    credentials = properties['teamcity.auth.userId'] + ':' + properties['teamcity.auth.password']
    build_id = properties['teamcity.build.id']
    require(re.fullmatch(r'[1-9][0-9]*', build_id), 'Invalid current build ID')
    request = urllib.request.Request(args.server + '/app/rest/builds/id:' + build_id +
                                    '?fields=branchName,revisions(revision(version)),artifact-dependencies(build('
                                    'id,number,buildTypeId,state,status,branchName,canceledInfo,revisions(revision(version))))',
                                    headers={'Accept': 'application/json', 'Authorization': 'Basic ' +
                                             base64.b64encode(credentials.encode()).decode()})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(request, timeout=30) as response:
        current = json.load(response)
    dependencies = current['artifact-dependencies']['build']
    require(len(dependencies) == 1, 'Expected one resolved Compile artifact dependency')
    revisions = current['revisions']['revision']
    require(len(revisions) == 1, 'Expected one PR revision')
    head = revisions[0]['version']
    pr = os.environ['PR_NUMBER']
    require(current['branchName'] == 'pull/' + pr, 'Requested PR differs from TeamCity branch')
    params = consumer_parameters(dependencies[0], raw, pr, head, os.environ['CI_BRANCH'], os.environ)
    for key, value in params.items():
        print("##teamcity[setParameter name='" + escape(key) + "' value='" + escape(value) + "']")
    print('Compile artifact preflight passed: producer=' + str(dependencies[0]['id']) + ', PR=' + pr + ', head=' + head)


if __name__ == '__main__':
    try:
        main()
    except (KeyError, TypeError, ValueError, OSError) as error:
        message = 'Compile artifact preflight failed: ' + str(error)
        print("##teamcity[buildProblem description='" + escape(message) + "']", flush=True)
        sys.exit(1)
