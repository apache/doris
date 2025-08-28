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

##############################################################
# This script is used to generate generated source code
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

. "${DORIS_HOME}/env.sh"

echo "Build generated code"
cd "${DORIS_HOME}/gensrc"

# if calling from build.sh, no need to clean build/ dir.
# it will be removed by using `build.sh --clean`.
# when run this script along, it will always remove the build/ dir.
if [[ "$#" == 0 ]]; then
    echo "rm -rf ${DORIS_HOME}/gensrc/build"
    rm -rf "${DORIS_HOME}/gensrc/build"
fi

# DO NOT using parallel make(-j) for gensrc
make -j
rm -rf "${DORIS_HOME}/fe/fe-common/src/main/java/org/apache/doris/thrift ${DORIS_HOME}/fe/fe-common/src/main/java/org/apache/parquet"
rm -rf "${DORIS_HOME}/fe/fe-core/src/main/java/org/apache/doris/thrift ${DORIS_HOME}/fe/fe-core/src/main/java/org/apache/parquet"

cp -r "build/gen_java/org/apache/doris/thrift" "${DORIS_HOME}/fe/fe-common/src/main/java/org/apache/doris"
cp -r "build/gen_java/org/apache/parquet" "${DORIS_HOME}/fe/fe-common/src/main/java/org/apache/"
cd "${DORIS_HOME}/"
echo "Done"
exit 0
