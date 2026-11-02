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

# Exercise the packaging function with a JDK selected only through PATH.
eval "$(sed -n '/^function stamp_fe_build_version() {$/,/^}$/p' "${script_dir}/../../build.sh")"
mkdir -p "${test_root}/gensrc/script" "${test_root}/lib" "${test_root}/jdk-bin"
cp "${script_dir}/gen_build_version.sh" "${test_root}/gensrc/script/"
"${JAVA_HOME}/bin/jar" cf "${test_root}/lib/fe-common-test.jar" -C "${test_root}/classes" org/apache/doris/common/Version.class
export VERSION_TEST_JDK_BIN="${JAVA_HOME}/bin"
export VERSION_TEST_TOOL_LOG="${test_root}/jdk-tools.log"
for tool in java javac jar; do
    cat >"${test_root}/jdk-bin/${tool}" <<'TOOL'
#!/usr/bin/env bash
printf '%s\n' "${0##*/}" >>"${VERSION_TEST_TOOL_LOG}"
exec "${VERSION_TEST_JDK_BIN}/${0##*/}" "$@"
TOOL
    chmod +x "${test_root}/jdk-bin/${tool}"
done
unset JAVA_HOME JDK_17
export PATH="${test_root}/jdk-bin:${PATH}"
(
    # Match build.sh, which permits unset optional environment variables.
    set +u
    DORIS_BUILD_VERSION_MAJOR=9 DORIS_BUILD_VERSION_MINOR=10 \
    DORIS_BUILD_VERSION_PATCH=11 DORIS_BUILD_VERSION_HOTFIX=12 \
        stamp_fe_build_version "${test_root}/lib"
)
for tool in javac jar; do
    if ! grep -qx "${tool}" "${VERSION_TEST_TOOL_LOG}"; then
        echo "FE version stamping bypassed the PATH-selected ${tool}" >&2
        exit 1
    fi
done
actual="$(java -cp "${test_root}/lib/fe-common-test.jar:${test_root}/classes" VersionReader)"
if [[ "${actual}" != "9.10.11.12" ]]; then
    echo "Stamped FE jar retained ${actual}; expected refreshed metadata 9.10.11.12" >&2
    exit 1
fi
echo "FE version stamping uses the PATH-selected JDK and refreshes the deployed jar"
