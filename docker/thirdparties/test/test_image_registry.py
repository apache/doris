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

"""Image-source contract tests; no daemon, credentials or network are needed."""
import csv
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
COMPONENTS = 'mysql,pg,oracle,sqlserver,clickhouse,es,hive2,hive3,iceberg,iceberg-rest,hudi,kafka,mariadb,db2,oceanbase,kerberos,ranger,polaris,minio,fluss,mongodb'
MAPPED = {
    'THIRDPARTY_IMAGE_MODE': 'mapped',
    'THIRDPARTY_IMAGE_REGISTRY': 'harbor.example:443',
    'THIRDPARTY_IMAGE_REGISTRY_MAP': 'docker.io=dockerhub,ghcr.io=ghcr,quay.io=quay,icr.io=icr,cgr.dev=cgr,mcr.microsoft.com=mcr',
}


class ImageRegistryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='doris-images-')
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(('THIRDPARTY_', 'FLUSS_', 'ICEBERG_'))}
        self.env.update(CONTAINER_UID='images-test-', IP_HOST='127.0.0.1', HOST_OS='Linux')
        self.docker_log = self.work / 'docker.log'
        self.env['IMAGE_TEST_LOG'] = str(self.docker_log)
        bindir = self.work / 'bin'
        bindir.mkdir()
        docker = bindir / 'docker'
        docker.write_text('''#!/usr/bin/env bash
printf '%s\\n' "$*" >>"${IMAGE_TEST_LOG}"
case "$1" in
info) echo '["name=rootless"]' ;;
image) [[ "${IMAGE_TEST_CACHED:-0}" == 1 ]] ;;
pull) [[ "${IMAGE_TEST_PULL_FAIL:-0}" == 0 ]] ;;
*) exit 99 ;;
esac
''')
        docker.chmod(0o755)
        self.env['PATH'] = str(bindir) + os.pathsep + self.env['PATH']

    def run_bash(self, script, *args, env=None, ok=True):
        result = subprocess.run(['bash', '-eo', 'pipefail', '-c', script, 'test', *args],
                                cwd=ROOT, env={**self.env, **(env or {})},
                                text=True, capture_output=True)
        if ok:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def resolve(self, reference, env=None, ok=True):
        return self.run_bash('. ./image-registry.sh; thirdparty_resolve_image "$1"',
                             reference, env=env, ok=ok)

    def inventory(self, components=COMPONENTS, env=None):
        result = self.run_bash('bash ./run-thirdparties-docker.sh -c "$1" --list-images',
                               components, env=env)
        rows = list(csv.DictReader(io.StringIO(result.stdout), delimiter='\t'))
        self.assertTrue(rows)
        return {row['VARIABLE']: row for row in rows}

    def test_native_preserves_references(self):
        for reference in ['mysql:8', 'apache/spark:4', 'ghcr.io/org/image:v1',
                          'image@sha256:' + 'a' * 64, 'registry.example:5000/team/image:v2']:
            with self.subTest(reference=reference):
                self.assertEqual(self.resolve(reference).stdout.strip(), reference)

    def test_mapping_and_digests(self):
        cases = {
            'mysql:8': 'dockerhub/library/mysql:8',
            'docker.io/mysql:8': 'dockerhub/library/mysql:8',
            'index.docker.io/library/mysql:8': 'dockerhub/library/mysql:8',
            'registry-1.docker.io/apache/spark:4': 'dockerhub/apache/spark:4',
            'doristhirdpartydocker/hive:v1': 'dockerhub/doristhirdpartydocker/hive:v1',
            'ghcr.io/org/image:v1': 'ghcr/org/image:v1',
            'quay.io/org/image:v2': 'quay/org/image:v2',
            'icr.io/db2_community/db2:11.5': 'icr/db2_community/db2:11.5',
            'cgr.dev/chainguard/image:latest': 'cgr/chainguard/image:latest',
            'mcr.microsoft.com/mssql/server:2022': 'mcr/mssql/server:2022',
            'apache/hive:4@sha256:' + 'a' * 64: 'dockerhub/apache/hive:4@sha256:' + 'a' * 64,
        }
        for reference, suffix in cases.items():
            with self.subTest(reference=reference):
                expected = 'harbor.example:443/' + suffix
                self.assertEqual(self.resolve(reference, MAPPED).stdout.strip(), expected)
                self.assertEqual(self.resolve(expected, MAPPED).stdout.strip(), expected)
        custom = {**MAPPED, 'THIRDPARTY_IMAGE_REGISTRY_MAP': 'registry.example:5000=offline/team'}
        self.assertEqual(self.resolve('registry.example:5000/app:v1', custom).stdout.strip(),
                         'harbor.example:443/offline/team/app:v1')

    def test_invalid_configuration(self):
        for override in [
            {'THIRDPARTY_IMAGE_MODE': 'typo'},
            {'THIRDPARTY_IMAGE_REGISTRY': ''},
            {'THIRDPARTY_IMAGE_REGISTRY': 'https://harbor.example'},
            {'THIRDPARTY_IMAGE_REGISTRY': 'harbor.example/project'},
            {'THIRDPARTY_IMAGE_REGISTRY_MAP': 'docker.io'},
            {'THIRDPARTY_IMAGE_REGISTRY_MAP': 'docker.io=../project'},
            {'THIRDPARTY_IMAGE_REGISTRY_MAP': 'docker.io=dockerhub,'},
            {'THIRDPARTY_IMAGE_REGISTRY_MAP': 'docker.io=dockerhub\nquay.io=quay'},
            {'THIRDPARTY_IMAGE_REGISTRY_MAP': 'docker.io=a,index.docker.io=b'},
            {'THIRDPARTY_IMAGE_REGISTRY_MAP': ''},
        ]:
            with self.subTest(override=override):
                self.resolve('mysql:8', {**MAPPED, **override}, ok=False)
        self.assertIn('no mapping for registry unknown.example',
                      self.resolve('unknown.example/image:1', MAPPED, ok=False).stderr)
        for reference in ['', 'https://docker.io/mysql:8', 'mysql:8 bad']:
            self.resolve(reference, MAPPED, ok=False)

    def test_selected_components_and_private_override(self):
        env = {**MAPPED, 'THIRDPARTY_IMAGE_REGISTRY_MAP': 'docker.io=dockerhub'}
        rows = self.inventory('mysql', env)
        self.assertEqual(set(rows), {'THIRDPARTY_MYSQL_IMAGE', 'THIRDPARTY_FILESYSTEM_IMAGE'})
        self.assertEqual(rows['THIRDPARTY_MYSQL_IMAGE']['TARGET'],
                         'harbor.example:443/dockerhub/library/mysql:5.7.36')
        self.run_bash('bash ./run-thirdparties-docker.sh -c ranger --list-images', env=env, ok=False)
        env['THIRDPARTY_CLICKHOUSE_IMAGE'] = 'harbor.example:443/doris/clickhouse-server:23.8'
        row = self.inventory('clickhouse', env)['THIRDPARTY_CLICKHOUSE_IMAGE']
        self.assertEqual(row['TARGET'], env['THIRDPARTY_CLICKHOUSE_IMAGE'])
        self.assertEqual(set(self.inventory('minio', env)), {'THIRDPARTY_MINIO_IMAGE'})
        self.assertFalse(self.docker_log.exists(), 'inventory contacted Docker')

    def test_local_build_and_platform_defaults(self):
        rows = self.inventory('kerberos', MAPPED)
        local = rows['THIRDPARTY_KERBEROS_IMAGE']
        self.assertEqual((local['KIND'], local['TARGET']), ('local', 'doris-kerberos-minimal:images-test-'))
        self.assertIn('/dockerhub/doristhirdpartydocker/hive@sha256:',
                      rows['THIRDPARTY_KERBEROS_BASE_IMAGE']['TARGET'])
        for platform, expected in [('Linux', 'postgis/postgis:14-3.3'), ('Darwin', 'postgres:14')]:
            # The launcher detects the host; call initialization directly to exercise Darwin.
            result = self.run_bash('. ./image-registry.sh; thirdparty_images_init iceberg; echo "$ICEBERG_POSTGRES_IMAGE"',
                                   env={'HOST_OS': platform})
            self.assertEqual(result.stdout.strip(), expected)
        result = self.run_bash('. ./image-registry.sh; thirdparty_images_init iceberg; echo "$ICEBERG_POSTGRES_IMAGE"',
                               env={**MAPPED, 'HOST_OS': 'Darwin', 'ICEBERG_POSTGRES_IMAGE': 'postgres:16'})
        self.assertEqual(result.stdout.strip(), 'harbor.example:443/dockerhub/library/postgres:16')

    def test_fluss_env_render_preserves_override(self):
        result = self.run_bash('''
. ./image-registry.sh
thirdparty_images_init fluss
envsubst < docker-compose/fluss/fluss.env.tpl > "$1"
. "$1"
printf '%s\\n' "$FLUSS_SERVER_IMAGE" "$FLUSS_FLINK_IMAGE"
''', str(self.work / 'fluss.env'), env={**MAPPED, 'FLUSS_SERVER_IMAGE': 'apache/fluss:custom'})
        self.assertEqual(result.stdout.splitlines(), [
            'harbor.example:443/dockerhub/apache/fluss:custom',
            'harbor.example:443/dockerhub/apache/fluss-quickstart-flink:1.20-1.0.0'])

    def test_capability_and_pull_commands(self):
        result = self.run_bash('bash ./run-thirdparties-docker.sh --image-config-version')
        self.assertEqual(result.stdout, '1\n')
        self.assertFalse(self.docker_log.exists())
        self.run_bash('bash ./run-thirdparties-docker.sh -c mysql --pull-images', env=MAPPED)
        self.assertEqual(self.docker_log.read_text().splitlines(), [
            "info --format {{json .SecurityOptions}}",
            'image inspect harbor.example:443/dockerhub/library/mysql:5.7.36',
            'pull harbor.example:443/dockerhub/library/mysql:5.7.36',
            'image inspect harbor.example:443/dockerhub/library/alpine:3.20',
            'pull harbor.example:443/dockerhub/library/alpine:3.20'])

    def test_pull_failure_precedes_fixture_mutation(self):
        result = self.run_bash('bash ./run-thirdparties-docker.sh -c mysql',
                               env={**MAPPED, 'IMAGE_TEST_PULL_FAIL': '1'}, ok=False)
        self.assertIn('pull failed: mysql THIRDPARTY_MYSQL_IMAGE', result.stderr)
        calls = self.docker_log.read_text().splitlines()
        self.assertEqual(len(calls), 3)
        self.assertFalse(any('compose' in call or call.startswith('run ') for call in calls))

    def test_invalid_cli_does_not_contact_docker(self):
        for args in ['--stop --pull-images', '--stop --list-images',
                     '--list-images --pull-images', '-c filesystem --pull-images',
                     '-c mysql,typo --pull-images', '-c mysql,,hive3 --list-images']:
            self.run_bash('bash ./run-thirdparties-docker.sh ' + args, env=MAPPED, ok=False)
        self.assertFalse(self.docker_log.exists())

    def test_cached_images_and_local_outputs_are_not_pulled(self):
        self.run_bash('bash ./run-thirdparties-docker.sh -c kerberos --pull-images',
                      env={**MAPPED, 'IMAGE_TEST_CACHED': '1'})
        calls = self.docker_log.read_text()
        self.assertNotIn('pull ', calls)
        self.assertNotIn('doris-kerberos-minimal', calls)

    def test_helper_uses_mapped_image(self):
        result = self.run_bash('''
. ./docker-command.sh
docker_cli() { printf '%s\\n' "$@"; }
rootless_fixture_cmd /tmp/fixture true
''', env=MAPPED)
        self.assertIn('harbor.example:443/dockerhub/library/alpine:3.20', result.stdout.splitlines())

    def test_snapshot_standalone_uses_mapped_image(self):
        # Export a Docker function to the child script, retaining the real
        # snapshot/configuration code while substituting only the daemon.
        result = self.run_bash('''
docker() {
    case "$1" in
    info) echo '["name=rootless"]' ;;
    volume) return 0 ;;
    run) printf '%s\\n' "$*" > "$IMAGE_TEST_LOG"; printf archive ;;
    *) return 99 ;;
    esac
}
export -f docker
bash docker-compose/hive/scripts/snapshot-hive-baseline.sh test-prefix "$1"
''', str(self.work / 'snapshot.tar.gz'), env=MAPPED)
        self.assertIn('[snapshot] saved', result.stdout)
        self.assertIn('harbor.example:443/dockerhub/library/alpine:3.20', self.docker_log.read_text())
        self.assertEqual((self.work / 'snapshot.tar.gz').read_text(), 'archive')

    def test_template_catalog_coverage(self):
        all_rows = self.inventory()
        referenced = set()
        templates = list((ROOT / 'docker-compose').rglob('*.yaml.tpl'))
        self.assertGreater(len(templates), 20)
        for template in templates:
            component = template.relative_to(ROOT / 'docker-compose').parts[0]
            component = {'postgresql': 'pg', 'elasticsearch': 'es'}.get(component, component)
            if component == 'hive':
                component = 'hive2' if '2x' in template.name else 'hive3'
            selected = self.inventory(component)
            references = re.findall(r'^\s*image:\s*(.+)$', template.read_text(), re.M)
            for reference in references:
                match = re.fullmatch(r'\$\{(\w+)\}', reference)
                self.assertIsNotNone(match, f'unregistered image in {template}: {reference}')
                variable = match.group(1)
                self.assertIn(variable, selected, template)
                referenced.add(variable)
        self.assertGreater(len(referenced), 40)
        self.assertEqual(set(all_rows) - referenced, {
            'THIRDPARTY_FILESYSTEM_IMAGE', 'THIRDPARTY_HIVE_FILESYSTEM_IMAGE', 'THIRDPARTY_KERBEROS_BASE_IMAGE'})
        dockerfile = (ROOT / 'docker-compose/kerberos/Dockerfile').read_text()
        self.assertEqual(re.findall(r'^FROM (.+)$', dockerfile, re.M), ['${BASE_IMAGE}'])
        self.assertIn('BASE_IMAGE: ${THIRDPARTY_KERBEROS_BASE_IMAGE}',
                      (ROOT / 'docker-compose/kerberos/kerberos.yaml.tpl').read_text())
        self.assertEqual(re.search(r'^ARG BASE_IMAGE=(.+)$', dockerfile, re.M).group(1),
                         all_rows['THIRDPARTY_KERBEROS_BASE_IMAGE']['SOURCE'])

    def test_compose_interpolates_all_image_variables(self):
        # Exercise real Compose interpolation of every declared service image
        # and build argument, without fixture data, env files, or a daemon.
        docker = shutil.which('docker')
        if not docker:
            self.skipTest('Docker CLI with Compose v2 is required')
        for config in [{}, MAPPED]:
            rows = self.inventory(env=config)
            services = {f'image{index}': {'image': '${' + key + '}'}
                        for index, key in enumerate(rows)}
            services['kerberos-build'] = {
                'image': '${THIRDPARTY_KERBEROS_IMAGE}',
                'build': {'context': '.', 'args': {'BASE_IMAGE': '${THIRDPARTY_KERBEROS_BASE_IMAGE}'}}}
            compose_file = self.work / 'compose.json'
            compose_file.write_text(json.dumps({'services': services}))
            result = subprocess.run([docker, 'compose', '-p', 'image-test', '-f', str(compose_file),
                                     'config', '--format', 'json'], capture_output=True, text=True,
                                    env={**self.env, **{key: row['TARGET'] for key, row in rows.items()}})
            self.assertEqual(result.returncode, 0, result.stderr)
            rendered = json.loads(result.stdout)['services']
            for index, row in enumerate(rows.values()):
                self.assertEqual(rendered[f'image{index}']['image'], row['TARGET'])
            self.assertEqual(rendered['kerberos-build']['build']['args']['BASE_IMAGE'],
                             rows['THIRDPARTY_KERBEROS_BASE_IMAGE']['TARGET'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
