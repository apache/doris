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

# Keep the check independent of the installed prefix: an external compilation image
# can carry old vars.sh alongside an ABI-compatible but behaviorally stale archive.
lance_c_install_fingerprint() (
    local definitions="$1"
    local TP_DIR="${definitions}"
    source "${definitions}/vars.sh" || return 1
    local patch_checksum
    patch_checksum="$(cksum < "${definitions}/patches/lance-c-foyer.patch")" || return 1
    printf '%s\n' "${LANCE_C_SOURCE}" "${LANCE_C_MD5SUM}" "${patch_checksum}"
)

lance_c_install_is_current() {
    local definitions="$1" installed="$2" expected
    [[ -s "${installed}/lib64/liblance_c.a" &&
       -s "${installed}/include/lance/lance.h" &&
       -s "${installed}/include/lance/lance.hpp" &&
       -s "${installed}/lib64/.lance-c-fingerprint" ]] || return 1
    expected="$(lance_c_install_fingerprint "${definitions}")" || return 1
    [[ "$(cat "${installed}/lib64/.lance-c-fingerprint")" == "${expected}" ]]
}
