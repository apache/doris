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

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

PROFILES = ('p0', 'nonConcurrent', 'cloud_p0', 'vault_p0', 'external')


def version(root, profile):
    assert profile in PROFILES
    pipeline = root / 'regression-test/pipeline'
    marker = pipeline / 'ci-config-version'
    assert marker.read_text() == '1\n', 'unsupported cases CI configuration version'
    cloud = profile in ('cloud_p0', 'vault_p0')
    files = (['fe_custom.conf', 'be_custom.conf', 'ms_custom.conf', 'recycler_custom.conf',
              'regression-conf-custom.groovy'] if cloud else
             ['fe.conf', 'be.conf', 'regression-conf.groovy'])
    for name in files:
        assert (pipeline / profile / 'conf' / name).is_file(), 'incomplete cases configuration: ' + name
    return 1


def override(raw, settings):
    lines = [line for line in raw.splitlines() if line.split('=', 1)[0].strip() not in settings]
    return '\n'.join(lines) + '\n' + ''.join(k + '=' + v + '\n' for k, v in settings.items())


def configure(root, report, profile, python_interpreter, build_id,
              data_dir=Path('/data/doris_cloud')):
    assert version(root, profile) == 1
    assert os.environ.get('DORIS_CI_PRISTINE_FRAMEWORK') == 'true'
    assert str(build_id).isdigit()
    conf = root / 'regression-test/pipeline' / profile / 'conf'
    out = root / 'output'
    cloud = profile in ('cloud_p0', 'vault_p0')
    # These are deployment-local addresses/paths, not feature flags or suite filters.
    if cloud:
        assert (out / 'ms/lib/doris_cloud').is_file()
        assert not (out / 'recycler').exists()
        shutil.copytree(out / 'ms', out / 'recycler')
        for component in ('fe', 'be'):
            shutil.copyfile(conf / (component + '_custom.conf'), out / component / 'conf' / (component + '_custom.conf'))
        for component in ('ms', 'recycler'):
            target = out / component / 'conf/doris_cloud.conf'
            custom = (conf / (component + '_custom.conf')).read_text()
            # Source custom settings must win over binary defaults, including conflict-key logging.
            keys = {line.split('=', 1)[0].strip() for line in custom.splitlines()
                    if '=' in line and not line.lstrip().startswith('#')}
            original = '\n'.join(line for line in target.read_text().splitlines()
                                 if line.split('=', 1)[0].strip() not in keys | {'fdb_cluster'})
            target.write_text(original + '\nfdb_cluster = dorisvalidation:dorisvalidation@127.0.0.1:4500\n' + custom)
        be = out / 'be/conf/be_custom.conf'
        be.write_text(override(be.read_text(), {'priority_networks': '127.0.0.1/32',
                                              'python_venv_interpreter_paths': python_interpreter}))
        for name in ('file_cache', 'tmp'):
            (data_dir / name).mkdir(parents=True, exist_ok=True)
    else:
        for component in ('fe', 'be'):
            settings = {'priority_networks': '127.0.0.1/32'}
            settings.update({'meta_dir': '/doris/local-data/meta'} if component == 'fe' else
                            {'storage_root_path': '/doris/local-data/storage1;/doris/local-data/storage2',
                             'python_venv_interpreter_paths': python_interpreter})
            if profile == 'p0':
                settings['trino_connector_plugin_dir'] = '/tmp/trino_connector/connectors'
            (out / component / 'conf' / (component + '.conf')).write_text(
                override((conf / (component + '.conf')).read_text(), settings))
        for name in ('meta', 'storage1', 'storage2', 'regression'):
            (root / 'local-data' / name).mkdir(parents=True, exist_ok=True)
    name = 'regression-conf-custom.groovy' if cloud else 'regression-conf.groovy'
    source = (conf / name).read_text()
    raw = source if cloud else source.replace('172.19.0.2', '127.0.0.1').replace(
        'cacheDataPath = "/data/regression/"', 'cacheDataPath = "/doris/local-data/regression/"')
    actual_version = subprocess.check_output([python_interpreter, '-c',
                       'import platform;print(platform.python_version())'], text=True).strip()
    dynamic = {'pythonUdfRuntimeVersion': actual_version}
    if profile in ('p0', 'cloud_p0', 'vault_p0'):
        dynamic['trinoPluginsPath'] = '/tmp/trino_connector'
    if profile == 'external':
        dynamic.update(ciFixtureBuildId=str(build_id), ciBackupRepositoryBuildId=str(build_id))
    raw += '\n' + ''.join(k + ' = ' + json.dumps(v) + '\n' for k, v in dynamic.items())
    raw += '\nak = System.getenv("AWS_ACCESS_KEY_ID")\nsk = System.getenv("AWS_SECRET_ACCESS_KEY")\nsessionToken = System.getenv("AWS_SESSION_TOKEN")\n'
    (root / 'regression-test/conf' / name).write_text(raw)
    report.mkdir(parents=True, exist_ok=True)
    (report / 'config-binding.json').write_text(json.dumps({'profile': profile, 'configVersion': 1,
        'sourceConfigSha256': hashlib.sha256(source.encode()).hexdigest(),
        'runtimeConfigSha256': hashlib.sha256(raw.encode()).hexdigest(),
        'scriptExclusionsAdded': False, 'pythonVersion': actual_version,
        'sourcePolicyFiles': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in sorted(conf.iterdir()) if p.is_file()}}, indent=2) + '\n')
    # Retain the original report paths and hash field names used by collectors.
    source_hash = hashlib.sha256(source.encode()).hexdigest()
    runtime_hash = hashlib.sha256(raw.encode()).hexdigest()
    if profile in ('p0', 'nonConcurrent'):
        (report / 'config-binding.txt').write_text(
            'profile=' + profile + '\nsource_sha256=' + source_hash +
            '\nruntime_sha256=' + runtime_hash + '\npython_udf=' + actual_version + '\nconfig_version=1\n')
    if cloud:
        binding = json.loads((report / 'config-binding.json').read_text())
        binding.update(source_config_sha256=source_hash, runtime_custom_config_sha256=runtime_hash,
                       base_config_sha256=hashlib.sha256((root / 'regression-test/conf/regression-conf.groovy').read_bytes()).hexdigest(),
                       selection_source=str(conf / name), new_exclusions=False,
                       original_case_files_changed=False, python_version=actual_version)
        (report / 'config-binding.json').write_text(json.dumps(binding, indent=2) + '\n')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Apply source-owned regression configuration')
    parser.add_argument('root', type=Path)
    parser.add_argument('report', type=Path)
    parser.add_argument('profile', choices=PROFILES)
    parser.add_argument('python_interpreter')
    parser.add_argument('build_id')
    args = parser.parse_args()
    configure(args.root, args.report, args.profile, args.python_interpreter, args.build_id)
