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

readonly APT_SOURCES_DIR="${APT_SOURCES_DIR:-/etc/apt}"

shopt -s nullglob
source_files=(
    "${APT_SOURCES_DIR}/sources.list"
    "${APT_SOURCES_DIR}/sources.list.d"/*.list
    "${APT_SOURCES_DIR}/sources.list.d"/*.sources
)

source_file_found=false
for source_file in "${source_files[@]}"; do
    [[ -f "${source_file}" ]] || continue
    source_file_found=true
    sed -E -i \
        -e 's#https?://security\.debian\.org/debian-security#https://archive.debian.org/debian-security#g' \
        -e 's#https?://deb\.debian\.org/debian-security#https://archive.debian.org/debian-security#g' \
        -e 's#https?://deb\.debian\.org/debian#https://archive.debian.org/debian#g' \
        "${source_file}"
done

if [[ "${source_file_found}" != "true" ]]; then
    echo "No APT source files found under ${APT_SOURCES_DIR}" >&2
    exit 1
fi
