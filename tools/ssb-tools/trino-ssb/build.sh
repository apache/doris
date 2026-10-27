#!/usr/bin/env bash
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

set -euo pipefail
SSB_PLUGIN_ROOT=$(cd "$(dirname "$0")" && pwd)
cd "${SSB_PLUGIN_ROOT}"
mkdir -p target/native

# The same generator distribution as ../bin/build-ssb-dbgen.sh. Pin its contents
# so a benchmark run cannot silently switch to a different data generator.
archive=target/ssb-dbgen-linux.tar.gz
if [[ ! -f "${archive}" ]]; then
    curl --fail --location --retry 3 \
        https://palo-cloud-repo-bd.bd.bcebos.com/baidu-doris-release/ssb-dbgen-linux.tar.gz \
        --output "${archive}"
fi
echo "9a28f5a24091fd55d7e2ea12850e0189d98d1e1c6361f84442353ebd7df8e0ef  ${archive}" | sha256sum --check
tar -xzf "${archive}" -C target/native --strip-components=1
# The legacy source contains trailing whitespace; keep that out of the patch.
patch -l -d target/native -p1 <dbgen-stream.patch
parallel=$(($(nproc) / 4))
parallel=$((parallel > 0 ? parallel : 1))
make -C target/native -j"${parallel}" CC="${CC:-gcc} -std=gnu89 -fcommon" dbgen
mvn -B -f pom.xml -T "${parallel}" package

mkdir -p target/ssb
cp target/trino-ssb.jar target/native/dbgen target/native/dists.dss target/ssb/
cp target/native/README target/native/TPCH_README target/ssb/
echo "SSB plugin: ${SSB_PLUGIN_ROOT}/target/ssb"
echo 'Install this directory under plugins/trino_plugins/ on every FE and BE before starting Doris.'
