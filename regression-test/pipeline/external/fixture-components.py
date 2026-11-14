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

"""Select native fixtures using only the reviewed source component policy."""
import hashlib
import json
from pathlib import Path
import re
import sys


def select(root, source):
    root = Path(root)
    launcher = (root / 'docker/thirdparties/run-thirdparties-docker.sh').read_text()

    def assignment(name):
        rows = re.findall(r'(?m)^' + name + r'="([^"\n]+)"$', launcher)
        if len(rows) != 1:
            raise ValueError('Unsupported source component declaration: ' + name)
        return rows[0]

    defaults = assignment('DEFAULT_COMPONENTS')
    all_components = assignment('ALL_COMPONENTS').replace('${DEFAULT_COMPONENTS}', defaults)
    if not re.fullmatch(r'[a-z0-9]+[-a-z0-9]*(?:,[a-z0-9]+[-a-z0-9]*)*', all_components):
        raise ValueError('Nonliteral source component declaration')
    selected = all_components.split(',')
    if len(set(selected)) != len(selected):
        raise ValueError('Repeated source components')
    policy = root / 'regression-test/pipeline/external/conf/components.json'
    omitted = json.loads(policy.read_text())['omitted_components']
    if not isinstance(omitted, dict) or any(name not in selected for name in omitted):
        raise ValueError('Unknown omitted component')
    for evidence in omitted.values():
        if not isinstance(evidence, dict) or set(evidence) != {'reason', 'restore_when'}:
            raise ValueError('Omission requires a reason and restoration condition')
        if any(not isinstance(value, str) or not value.strip() for value in evidence.values()):
            raise ValueError('Empty omission rationale')
    selected = [name for name in selected if name not in omitted]
    if not selected:
        raise ValueError('No requested components remain')
    if 'mysql' in omitted and any(name in selected for name in ('hive2', 'hive3')):
        raise ValueError('Hive source automatically starts its required MySQL component')
    config = root / 'regression-test/pipeline/external/conf/regression-conf.groovy'
    return selected, {
        'source_sha': source,
        'config_sha256': hashlib.sha256(config.read_bytes()).hexdigest(),
        'component_policy_sha256': hashlib.sha256(policy.read_bytes()).hexdigest(),
        'selection_source': 'run-thirdparties-docker.sh:ALL_COMPONENTS',
        'original_case_selection_changed': False,
        'omitted_components': omitted,
        'full_dependency_selection': not bool(omitted),
    }


if __name__ == '__main__':
    components, receipt = select(sys.argv[1], sys.argv[2])
    receipt['components'] = components
    Path(sys.argv[3]).write_text(json.dumps(receipt, indent=2) + '\n')
    print(','.join(components))
