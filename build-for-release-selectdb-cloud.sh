#!/usr/bin/env bash
#
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

set -eo pipefail

echo "Begin to build for selectdb cloud with $*"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
export DORIS_HOME="${ROOT}"

. "${DORIS_HOME}/build-support/selectdb-release-common.sh"

usage() {
    echo "
Usage: $0 <options> [-- <build.sh options>]
  Optional options:
    --vendor <vendor>                  build version prefix, default cloud
    --disable-tde                      disable TDE feature modules
    --disable-tls                      disable TLS feature modules
    --disable-variant-nested-group     disable Variant NestedGroup feature module
    --disable-snapshot                 disable Snapshot feature modules
    --help                             print this help message

  Eg.
    $0 --vendor selectdb
    $0 --vendor selectdb --disable-tls
    $0 --vendor selectdb -- --cloud
"
}

VENDOR=
HELP=0
ENABLE_TDE=1
ENABLE_TLS=1
ENABLE_VARIANT_NESTED_GROUP=1
ENABLE_SNAPSHOT=1
args_remain=()

while [[ $# -gt 0 ]]; do
    case "$1" in
    --vendor)
        VENDOR="$2"
        shift 2
        ;;
    --help)
        HELP=1
        shift
        ;;
    --disable-tde)
        ENABLE_TDE=0
        shift
        ;;
    --disable-tls)
        ENABLE_TLS=0
        shift
        ;;
    --disable-variant-nested-group)
        ENABLE_VARIANT_NESTED_GROUP=0
        shift
        ;;
    --disable-snapshot)
        ENABLE_SNAPSHOT=0
        shift
        ;;
    --)
        shift
        args_remain+=("$@")
        break
        ;;
    *)
        args_remain+=("$1")
        shift
        ;;
    esac
done

if [[ "${HELP}" -eq 1 ]]; then
    usage
    exit 0
fi

export DISABLE_BUILD_UI="${DISABLE_BUILD_UI:-ON}"
export ENABLE_HDFS_STORAGE_VAULT="${ENABLE_HDFS_STORAGE_VAULT:-OFF}"

# These env vars are used by gensrc/script/gen_build_version.sh.
export DORIS_BUILD_VERSION_PREFIX="${VENDOR:-cloud}"
export DORIS_BUILD_VERSION_MAJOR="${DORIS_BUILD_VERSION_MAJOR:-26}"
export DORIS_BUILD_VERSION_MINOR="${DORIS_BUILD_VERSION_MINOR:-1}"
export DORIS_BUILD_VERSION_PATCH="${DORIS_BUILD_VERSION_PATCH:-0}"
export DORIS_BUILD_VERSION_HOTFIX="${DORIS_BUILD_VERSION_HOTFIX:-0}"
export DORIS_BUILD_VERSION_RC_VERSION="${DORIS_BUILD_VERSION_RC_VERSION:-}"

selectdb_configure_extra_modules \
    "${ENABLE_TDE}" \
    "${ENABLE_TLS}" \
    "${ENABLE_VARIANT_NESTED_GROUP}" \
    "${ENABLE_SNAPSHOT}"

echo "Get params:
    VENDOR                      -- ${VENDOR:-cloud}
    ENABLE_TDE                  -- ${ENABLE_TDE}
    ENABLE_TLS                  -- ${ENABLE_TLS}
    ENABLE_VARIANT_NESTED_GROUP -- ${ENABLE_VARIANT_NESTED_GROUP}
    ENABLE_SNAPSHOT             -- ${ENABLE_SNAPSHOT}
    EXTRA_FE_MODULES            -- ${EXTRA_FE_MODULES}
    EXTRA_BE_MODULES            -- ${EXTRA_BE_MODULES}
    EXTRA_CLOUD_MODULES         -- ${EXTRA_CLOUD_MODULES}
    build.sh args               -- ${args_remain[*]}
"

sh build.sh "${args_remain[@]}"
selectdb_copy_extra_fe_libs output

if [[ -f output/ms/lib/doris_cloud ]]; then
    ln -srf output/ms/lib/doris_cloud output/ms/lib/selectdb_cloud
fi
if [[ -f "${DORIS_HOME}/cloud/script/custom_start.sh" && -d output/ms/bin ]]; then
    cp "${DORIS_HOME}/cloud/script/custom_start.sh" output/ms/bin/
fi

echo "Succeed to build for selectdb cloud with $*"
