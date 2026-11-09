#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail
common=$(cd "$(dirname "$0")/.." && pwd)
source "$common/doris-utils.sh"
source "$common/github-utils.sh"
source "$common/oss-utils.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export DORIS_CI_ENV_MODE=preconfigured JAVA_HOME="$tmp/jdk" DORIS_CI_JAVA_MAJOR=17
mkdir -p "$JAVA_HOME/bin" "$tmp/reports" "$tmp/doris/be/bin"
printf '#!/bin/sh\necho "java.specification.version = 17" >&2\n' > "$JAVA_HOME/bin/java"
cp "$JAVA_HOME/bin/java" "$JAVA_HOME/bin/javac"
chmod +x "$JAVA_HOME/bin/"*
expect_failure() {
    if "$@"; then echo "Expected failure: $*" >&2; exit 1; fi
}
sudo() { echo 'Unexpected sudo' >&2; exit 99; }
sysctl() { echo 'Unexpected sysctl write' >&2; exit 99; }
swapoff() { echo 'Unexpected swapoff' >&2; exit 99; }
install_java
DORIS_CI_JAVA_MAJOR=8 expect_failure install_java
JAVA_HOME="$tmp/missing" expect_failure install_java
DORIS_CI_ENV_MODE=typo expect_failure install_java

# Fixtures model the kernel interfaces, never change the host running this test.
map_count=2000000 fixture_nofile=200000 core=unlimited swaps=disabled
cat() {
    if [[ "${1:-}" == /proc/sys/vm/max_map_count ]]; then printf '%s\n' "$map_count"; else command cat "$@"; fi
}
ulimit() {
    case "$1" in -Sn) printf '%s\n' "$fixture_nofile" ;; -Sc) printf '%s\n' "$core" ;; *) return 99 ;; esac
}
awk() {
    if [[ "${2:-}" == /proc/swaps ]]; then
        [[ "$swaps" != unreadable ]] || return 2
        if [[ "$swaps" == disabled ]]; then
            printf 'Filename Type Size Used Priority\n' | command awk "$1"
        else
            printf 'Filename Type Size Used Priority\n/swap file 100 0 -2\n' | command awk "$1"
        fi
    else command awk "$@"; fi
}
check_preconfigured_be_limits
map_count=1999999 expect_failure check_preconfigured_be_limits
map_count=unknown expect_failure check_preconfigured_be_limits
fixture_nofile=1024 expect_failure check_preconfigured_be_limits
core=0 expect_failure check_preconfigured_be_limits
swaps=enabled expect_failure check_preconfigured_be_limits
swaps=unreadable expect_failure check_preconfigured_be_limits
fixture_nofile=unlimited check_preconfigured_be_limits
export DORIS_HOME="$tmp/doris"
printf '#!/bin/sh\nexit 23\n' > "$DORIS_HOME/be/bin/start_be.sh"
chmod +x "$DORIS_HOME/be/bin/start_be.sh"
llvm-symbolizer() { return 0; }
expect_failure start_doris_be
# The failure must not be hidden by a subsequent readiness check.
printf '#!/bin/sh\ntouch "$DORIS_HOME/started"\n' > "$DORIS_HOME/be/bin/start_be.sh"
pgrep() { return 0; }
sleep() { return 0; }
start_doris_be
[[ -f "$DORIS_HOME/started" ]]

# Keep FE JaCoCo enabled and pass the configured local agent to the original launcher.
mkdir -p "$DORIS_HOME/fe/bin" "$DORIS_HOME/fe/conf"
printf 'query_port=9030\n' > "$DORIS_HOME/fe/conf/fe.conf"
export DORIS_CI_JACOCO_AGENT="$tmp/jacocoagent.jar"
touch "$DORIS_CI_JACOCO_AGENT"
cat > "$DORIS_HOME/fe/bin/start_fe.sh" <<'FE'
#!/bin/bash
printf '%s\n' "$JACOCO_COVERAGE_OPT" > "$DORIS_HOME/fe-agent.txt"
FE
chmod +x "$DORIS_HOME/fe/bin/start_fe.sh"
mysql() { printf 'Version: fixture-version\n'; }
start_doris_fe
grep -F -- "-javaagent:$DORIS_CI_JACOCO_AGENT=" "$DORIS_HOME/fe-agent.txt" >/dev/null
DORIS_CI_JACOCO_AGENT="$tmp/missing.jar" expect_failure start_doris_fe

export DORIS_CI_REPORT_BACKEND=artifact DORIS_CI_REPORT_DIR="$tmp/reports"
unset GITHUB_TOKEN
create_an_issue_comment 1 $'body with "quotes"\nand two lines'
[[ $(command cat "$tmp/reports/"*) == $'body with "quotes"\nand two lines' ]]
DORIS_CI_REPORT_BACKEND=github expect_failure create_an_issue_comment 1 body
DORIS_CI_REPORT_BACKEND=typo expect_failure create_an_issue_comment 1 body
DORIS_CI_REPORT_DIR="$tmp/missing" expect_failure create_an_issue_comment 1 body

export DORIS_CI_OSSUTIL="$tmp/ossutil" OSS_ACCESS_KEY_ID=TEST_ONLY_ID OSS_ACCESS_KEY_SECRET=TEST_ONLY_SECRET OSS_SESSION_TOKEN=TEST_ONLY_SESSION
export OSS_REGION=example-region OSS_ENDPOINT=https://example.invalid DORIS_CI_LOG_OSS_PREFIX=oss://example-bucket/build-1
export TEST_OSS_ARGS="$tmp/oss-args"
cat > "$DORIS_CI_OSSUTIL" <<'TOOL'
#!/bin/bash
if [[ "$1" == version ]]; then echo 'ossutil version: 2.4.1'; exit 0; fi
printf '%s\n' "$@" >> "$TEST_OSS_ARGS"
[[ "${TEST_UPLOAD_FAIL:-false}" != true ]]
TOOL
chmod +x "$DORIS_CI_OSSUTIL"
printf 'sanitized log\n' > "$tmp/log.txt"
upload_doris_log_to_oss "$tmp/log.txt"
! grep -q 'TEST_ONLY' "$TEST_OSS_ARGS"
grep -q 'oss://example-bucket/build-1/log.txt' "$TEST_OSS_ARGS"
OSS_SESSION_TOKEN='' expect_failure upload_doris_log_to_oss "$tmp/log.txt"
OSS_ENDPOINT=http://example.invalid expect_failure upload_doris_log_to_oss "$tmp/log.txt"
TEST_UPLOAD_FAIL=true expect_failure upload_doris_log_to_oss "$tmp/log.txt"
printf 'PASS: preconfigured runtime, startup errors, explicit report routing and STS-only log upload\n'
