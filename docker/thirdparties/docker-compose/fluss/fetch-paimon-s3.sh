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

################################################################
# Fetches the one jar the official fluss server image does not carry and the
# fluss servers need to reach an s3:// lake warehouse: paimon-s3, at the paimon
# version fluss.env.tpl pins (FLUSS_PAIMON_VERSION). It is put in cache/ beside
# this script, at the path the compose file bind mounts into the coordinator and
# the tablet server (FLUSS_PAIMON_S3_JAR in fluss.env.tpl spells the same name).
# The fluss project's own lakehouse quickstart mounts the jar the same way.
#
# Optional:
#   MAVEN_REPO         local maven repository (default ~/.m2/repository), looked
#                      in before anything is downloaded
#   FLUSS_ARTIFACT_CACHE  where the jar is kept between runs (default cache/
#                      beside this script, which git ignores)
################################################################

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# The paimon version lives in fluss.env.tpl so the compose file and this script
# cannot drift apart. The template's other entries reference variables that are
# empty here; only that literal is read.
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/fluss.env.tpl"
FLUSS_PAIMON_VERSION="${FLUSS_PAIMON_VERSION:?fluss.env.tpl must set FLUSS_PAIMON_VERSION}"

MAVEN_REPO="${MAVEN_REPO:-${HOME}/.m2/repository}"
FLUSS_ARTIFACT_CACHE="${FLUSS_ARTIFACT_CACHE:-${SCRIPT_DIR}/cache}"
MAVEN_CENTRAL_URL="https://repo1.maven.org/maven2"

# Prints a file's sha1 with whichever tool the host has (coreutils or BSD).
sha1_of() {
    if command -v sha1sum >/dev/null 2>&1; then
        sha1sum "$1" | cut -d' ' -f1
    else
        shasum -a 1 "$1" | cut -d' ' -f1
    fi
}

# Resolves one released maven artifact into the cache. The local repository is
# looked in first, so a machine that has built Doris needs no network for what
# that build already fetched; then the cache itself; and only then central,
# checked against the .sha1 it publishes so that a truncated download cannot
# poison the cache.
resolve_maven_artifact() {
    local group_path="$1" artifact="$2" version="$3" extension="$4"
    local file="${artifact}-${version}.${extension}"
    local local_path="${MAVEN_REPO}/${group_path}/${artifact}/${version}/${file}"
    local cached="${FLUSS_ARTIFACT_CACHE}/${file}"
    local url="${MAVEN_CENTRAL_URL}/${group_path}/${artifact}/${version}/${file}"
    local expected actual

    mkdir -p "${FLUSS_ARTIFACT_CACHE}"
    if [[ -f "${cached}" ]]; then
        echo "  ${file} (already in ${FLUSS_ARTIFACT_CACHE})"
        return 0
    fi
    if [[ -f "${local_path}" ]]; then
        cp "${local_path}" "${cached}"
        echo "  ${file} (from ${MAVEN_REPO})"
        return 0
    fi

    echo "  ${file} (downloading ${url})"
    if ! expected="$(curl -fsSL --retry 3 "${url}.sha1" | tr -d '[:space:]' | cut -c1-40)"; then
        echo "ERROR: could not fetch ${url}.sha1" >&2
        return 1
    fi
    if ! curl -fsSL --retry 3 -o "${cached}.tmp" "${url}"; then
        rm -f "${cached}.tmp"
        echo "ERROR: could not download ${url}" >&2
        return 1
    fi
    actual="$(sha1_of "${cached}.tmp")"
    if [[ "${actual}" != "${expected}" ]]; then
        rm -f "${cached}.tmp"
        echo "ERROR: sha1 mismatch for ${file}: repository says ${expected}, downloaded ${actual}" >&2
        return 1
    fi
    mv "${cached}.tmp" "${cached}"
}

echo "Fetching the paimon-s3 plugin for the fluss servers"
resolve_maven_artifact "org/apache/paimon" "paimon-s3" "${FLUSS_PAIMON_VERSION}" "jar"
# The servers run as uid 9999 (see the compose file) and read the jar through a
# bind mount, so it has to be readable by a user other than the one running this.
chmod 644 "${FLUSS_ARTIFACT_CACHE}/paimon-s3-${FLUSS_PAIMON_VERSION}.jar"
