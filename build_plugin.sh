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

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

. "${DORIS_HOME}/env.sh"

# Check args
usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --plugin           build special plugin
  Eg.
    $0 --plugin xxx     build xxx plugin
    $0                  build all plugins
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -o 'h' \
    -l 'plugin' \
    -l 'clean' \
    -l 'help' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

ALL_PLUGIN=1
CLEAN=0
HELP=0
if [[ "$#" == 1 ]]; then
    # defuat
    ALL_PLUGIN=1
    CLEAN=0
else
    while true; do
        case "$1" in
        --plugin)
            ALL_PLUGIN=0
            shift
            ;;
        --clean)
            CLEAN=1
            shift
            ;;
        -h)
            HELP=1
            shift
            ;;
        --help)
            HELP=1
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error"
            exit 1
            ;;
        esac
    done
fi

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

echo "Get params:
    BUILD_ALL_PLUGIN       -- ${ALL_PLUGIN}
    CLEAN                  -- ${CLEAN}
"

cd "${DORIS_HOME}"
PLUGIN_MODULE=''
if [[ "${ALL_PLUGIN}" -eq 1 ]]; then
    cd "${DORIS_HOME}/fe_plugins"
    if [[ "${CLEAN}" -eq 1 ]]; then
        "${MVN_CMD}" clean
    fi
    echo "build all plugins"
    "${MVN_CMD}" package -DskipTests
else
    PLUGIN_MODULE="$1"
    cd "${DORIS_HOME}/fe_plugins/${PLUGIN_MODULE}"
    if [[ "${CLEAN}" -eq 1 ]]; then
        "${MVN_CMD}" clean
    fi
    echo "build plugin ${PLUGIN_MODULE}"
    "${MVN_CMD}" package -DskipTests
fi

cd "${DORIS_HOME}"
# Clean and prepare output dir
DORIS_OUTPUT="${DORIS_HOME}/fe_plugins/output"
mkdir -p "${DORIS_OUTPUT}"

if [[ "${ALL_PLUGIN}" -eq 1 ]]; then
    cp -p "${DORIS_HOME}/fe_plugins"/*/target/*.zip "${DORIS_OUTPUT}"/
else
    cp -p "${DORIS_HOME}/fe_plugins/${PLUGIN_MODULE}/target"/*.zip "${DORIS_OUTPUT}"/
fi

echo "***************************************"
echo "Successfully build Doris FE Plugin"
echo "***************************************"

exit 0
