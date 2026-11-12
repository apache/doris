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

No network or credentials here: teamcity-utils.sh fetches these three documents
through the existing authenticated REST helper. Consumers verify package hashes
again after download. The compiled base/merge are pinned from the manifest; a
later advance of the target branch does not invalidate the same PR head.
"""
import datetime
import hashlib
import json
from pathlib import Path
import re
import sys


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


if __name__ == '__main__':
    directory = Path(sys.argv[1])
    try:
        parameters = consumer_parameters(json.loads((directory / 'build.json').read_bytes()),
                                         (directory / 'manifest.json').read_bytes(),
                                         json.loads((directory / 'artifacts.json').read_bytes()),
                                         *sys.argv[2:5])
    except (KeyError, TypeError, ValueError) as error:
        sys.exit('Cannot use Compile artifacts: ' + str(error))
    print(json.dumps(parameters))
