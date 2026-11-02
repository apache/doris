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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "${test_root}"' EXIT
export DORIS_HOME="${test_root}"
export JAVA_HOME="${JAVA_HOME:?JAVA_HOME must point to a JDK}"

# A cached consumer must observe refreshed metadata without being recompiled.
DORIS_BUILD_VERSION_MAJOR=1 DORIS_BUILD_VERSION_MINOR=2 \
DORIS_BUILD_VERSION_PATCH=3 DORIS_BUILD_VERSION_HOTFIX=4 \
    bash "${script_dir}/gen_build_version.sh"
version_java="${test_root}/fe/fe-common/target/generated-sources/build/org/apache/doris/common/Version.java"
cat >"${test_root}/VersionReader.java" <<'JAVA'
import org.apache.doris.common.Version;
public class VersionReader {
    public static void main(String[] args) {
        System.out.println(Version.DORIS_BUILD_VERSION_MAJOR + "." + Version.DORIS_BUILD_VERSION_MINOR
                + "." + Version.DORIS_BUILD_VERSION_PATCH + "." + Version.DORIS_BUILD_VERSION_HOTFIX);
    }
}
JAVA
"${JAVA_HOME}/bin/javac" --release 17 -d "${test_root}/classes" \
    "${version_java}" "${test_root}/VersionReader.java"
rm -f "${version_java}"
DORIS_BUILD_VERSION_MAJOR=5 DORIS_BUILD_VERSION_MINOR=6 \
DORIS_BUILD_VERSION_PATCH=7 DORIS_BUILD_VERSION_HOTFIX=8 \
    bash "${script_dir}/gen_build_version.sh"
"${JAVA_HOME}/bin/javac" --release 17 -d "${test_root}/classes" "${version_java}"
actual="$("${JAVA_HOME}/bin/java" -cp "${test_root}/classes" VersionReader)"
if [[ "${actual}" != "5.6.7.8" ]]; then
    echo "Cached version consumer retained ${actual}; expected refreshed metadata 5.6.7.8" >&2
    exit 1
fi
echo "Cached version consumer observes refreshed numeric metadata"
