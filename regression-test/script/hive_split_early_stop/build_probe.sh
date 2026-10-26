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
: "${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
split_probe_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
split_probe_output=${1:?Usage: build_probe.sh OUTPUT_DIRECTORY}
mkdir -p -- "${split_probe_output}/probe-classes"
"${JAVA_HOME}/bin/javac" --add-modules jdk.attach \
    --add-exports java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED \
    -d "${split_probe_output}/probe-classes" "${split_probe_dir}/SplitProbe.java"
cat > "${split_probe_output}/probe-manifest.mf" <<'MANIFEST'
Premain-Class: SplitProbe
Agent-Class: SplitProbe
Main-Class: SplitProbe
MANIFEST
"${JAVA_HOME}/bin/jar" cfm "${split_probe_output}/split-probe.jar" \
    "${split_probe_output}/probe-manifest.mf" -C "${split_probe_output}/probe-classes" .
