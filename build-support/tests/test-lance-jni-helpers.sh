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

# Packaging boundary tests use a synthetic source archive and compiler output; native execution is
# covered by the Lance FE tests. No network, compiler, or Linux host is needed for these checks.
set -euo pipefail

TASK_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TASK_FIXTURE="$(mktemp -d)"
trap 'rm -rf -- "${TASK_FIXTURE}"' EXIT
source "${TASK_REPO_ROOT}/docker/thirdparties/lance-jni-helpers.sh"

# Exercise the real build helper's JDK selection before replacing it for packaging tests.
(
    export TASK_JDK_HOME="${TASK_FIXTURE}/selected jdk"
    export TASK_JAVAC_CALLED="${TASK_FIXTURE}/javac-called"
    mkdir -p "${TASK_JDK_HOME}/bin"
    cat > "${TASK_JDK_HOME}/bin/java" <<'EOF'
#!/usr/bin/env bash
set -e
test "$1" = -XshowSettings:properties
test "$2" = -version
printf '    java.home = %s\n' "${TASK_JDK_HOME}" >&2
EOF
    cat > "${TASK_JDK_HOME}/bin/javac" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$0" > "${TASK_JAVAC_CALLED}"
EOF
    chmod +x "${TASK_JDK_HOME}/bin/java" "${TASK_JDK_HOME}/bin/javac"
    # Invoked by the build helper through CXX.
    # shellcheck disable=SC2329
    lance_test_cxx() {
        local arg saw_include=0
        for arg in "$@"; do
            if [[ "${arg}" == "-I${TASK_JDK_HOME}/include" ]]; then
                saw_include=1
            fi
        done
        test "${saw_include}" = 1
    }
    export CXX=lance_test_cxx
    for selection in home selected-java path; do
        unset JAVA_HOME JAVA
        case "${selection}" in
            home) export JAVA_HOME="${TASK_JDK_HOME}" ;;
            selected-java) export JAVA="${TASK_JDK_HOME}/bin/java" ;;
            path) export PATH="${TASK_JDK_HOME}/bin:${PATH}" ;;
            *) exit 1 ;;
        esac
        rm -f "${TASK_JAVAC_CALLED}"
        lance_arrow_cdata_jni_build "${TASK_FIXTURE}/source" "${TASK_FIXTURE}/arrow.jar" \
            "${TASK_FIXTURE}/build" Linux "${TASK_FIXTURE}/library.so"
        test "$(cat "${TASK_JAVAC_CALLED}")" = "${TASK_JDK_HOME}/bin/javac"
    done
)

mkdir -p "${TASK_FIXTURE}/lib" "${TASK_FIXTURE}/tp/installed/lance-jni" \
    "${TASK_FIXTURE}/jar/arrow_cdata_jni/x86_64" "${TASK_FIXTURE}/source/arrow-java/c"
printf 'source fixture\n' > "${TASK_FIXTURE}/source/arrow-java/c/fixture"
tar -czf "${TASK_FIXTURE}/tp/installed/lance-jni/${LANCE_ARROW_SOURCE_ASSET}" \
    -C "${TASK_FIXTURE}/source" arrow-java
LANCE_ARROW_SOURCE_SHA256="$(sha256sum "${TASK_FIXTURE}/tp/installed/lance-jni/${LANCE_ARROW_SOURCE_ASSET}" | awk '{print $1}')"
printf 'original-linux\n' > "${TASK_FIXTURE}/jar/arrow_cdata_jni/x86_64/libarrow_cdata_jni.so"
printf 'original-darwin\n' > "${TASK_FIXTURE}/jar/arrow_cdata_jni/x86_64/libarrow_cdata_jni.dylib"
TASK_JAR="${TASK_FIXTURE}/lib/arrow-c-data-${LANCE_ARROW_VERSION}.jar"
(cd "${TASK_FIXTURE}/jar" && zip -qr "${TASK_JAR}" arrow_cdata_jni)

lance_arrow_cdata_jni_build() {
    test -f "$1/c/fixture"
    printf 'rebuilt-linux\n' > "$5"
}
readelf() { printf 'Dynamic section has no C++ runtime dependencies\n'; }
curl() { echo 'test forbids network access' >&2; return 1; }

lance_arrow_cdata_jni_replace "${TASK_FIXTURE}/lib" "${TASK_FIXTURE}/tp" Linux x86_64
test "$(unzip -p "${TASK_JAR}" arrow_cdata_jni/x86_64/libarrow_cdata_jni.so)" = rebuilt-linux
test "$(unzip -p "${TASK_JAR}" arrow_cdata_jni/x86_64/libarrow_cdata_jni.dylib)" = original-darwin
TASK_GOOD_HASH="$(sha256sum "${TASK_JAR}" | awk '{print $1}')"

readelf() { printf 'NEEDED Shared library: [libstdc++.so.6]\n'; }
if lance_arrow_cdata_jni_replace "${TASK_FIXTURE}/lib" "${TASK_FIXTURE}/tp" Linux x86_64; then
    echo 'FAIL: accepted a shared C++ runtime dependency' >&2
    exit 1
fi
test "$(sha256sum "${TASK_JAR}" | awk '{print $1}')" = "${TASK_GOOD_HASH}"
readelf() { return 1; }
if lance_arrow_cdata_jni_replace "${TASK_FIXTURE}/lib" "${TASK_FIXTURE}/tp" Linux x86_64; then
    echo 'FAIL: accepted failed ELF inspection' >&2
    exit 1
fi
test "$(sha256sum "${TASK_JAR}" | awk '{print $1}')" = "${TASK_GOOD_HASH}"

LANCE_ARROW_SOURCE_SHA256=incorrect
if lance_arrow_cdata_jni_replace "${TASK_FIXTURE}/lib" "${TASK_FIXTURE}/tp" Linux x86_64; then
    echo 'FAIL: accepted an unverified source archive' >&2
    exit 1
fi
test "$(sha256sum "${TASK_JAR}" | awk '{print $1}')" = "${TASK_GOOD_HASH}"

mv "${TASK_JAR}" "${TASK_FIXTURE}/lib/arrow-c-data-wrong.jar"
if lance_arrow_cdata_jni_replace "${TASK_FIXTURE}/lib" "${TASK_FIXTURE}/tp" Linux x86_64; then
    echo 'FAIL: accepted the wrong Arrow version' >&2
    exit 1
fi
# Other targets retain their upstream libraries and need neither a local compiler nor an Arrow jar.
lance_arrow_cdata_jni_replace "${TASK_FIXTURE}/missing" "${TASK_FIXTURE}/tp" Darwin aarch64
echo 'PASS: JDK selection, JNI replacement, platform isolation, version/checksum/runtime gates, failure atomicity'
