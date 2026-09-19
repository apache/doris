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
# Builds the two images the fluss regression stack runs on.
#
# Fluss 1.0.0 is a release candidate: its artifacts are staged on
# repository.apache.org for the release vote rather than published to maven
# central, and no official image exists for it yet, so neither image can be
# pulled. Both are built from the staged artifacts, at the version
# fluss.env.tpl pins: the server image from the fluss-dist tarball, the flink
# image from the flink connector, the lake tiering job and fluss-lake-paimon,
# plus paimon and hadoop from maven central. Nothing here needs a fluss source
# checkout. Once 1.0.0 is released the fluss artifacts come from central
# instead (FLUSS_MAVEN_REPO_URL in fluss.env.tpl says where they come from),
# and the official apache/fluss image can replace the local server build.
#
# Optional:
#   FLINK_BASE_IMAGE   base Flink image (default flink:1.20.3-scala_2.12-java17,
#                      the Flink the pinned connector is built against)
#   FLUSS_FLINK_CONNECTOR_ARTIFACT  fluss connector artifact matching the base
#                      image (default fluss-flink-1.20)
#   FLUSS_DOCKER_REUSE_IMAGES  1 = skip an image whose tag already exists
#                              (decided per image; delete a tag to rebuild just it)
#   MAVEN_REPO         local maven repository (default ~/.m2/repository), looked
#                      in before anything is downloaded
#   FLUSS_ARTIFACT_CACHE  where downloads are kept between runs (default cache/
#                      beside this script, which git ignores)
################################################################

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# The fluss version and repository, the image tags and the paimon/hadoop
# versions live in fluss.env.tpl so the compose file and this script cannot
# drift apart. The template's other entries reference variables that are empty
# here; only those literals are read.
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/fluss.env.tpl"

FLINK_BASE_IMAGE="${FLINK_BASE_IMAGE:-flink:1.20.3-scala_2.12-java17}"
FLUSS_FLINK_CONNECTOR_ARTIFACT="${FLUSS_FLINK_CONNECTOR_ARTIFACT:-fluss-flink-1.20}"
MAVEN_REPO="${MAVEN_REPO:-${HOME}/.m2/repository}"
FLUSS_ARTIFACT_CACHE="${FLUSS_ARTIFACT_CACHE:-${SCRIPT_DIR}/cache}"
MAVEN_CENTRAL_URL="https://repo1.maven.org/maven2"
MAVEN_SNAPSHOTS_URL="https://repository.apache.org/content/repositories/snapshots"
# Where org/apache/fluss releases are served from: the staging repository the
# template names while 1.0.0 is a release candidate, central once the line is
# gone from the template because the release reached it. A trailing slash is
# dropped so that the URLs built below never carry a double one.
FLUSS_MAVEN_REPO_URL="${FLUSS_MAVEN_REPO_URL:-${MAVEN_CENTRAL_URL}}"
FLUSS_MAVEN_REPO_URL="${FLUSS_MAVEN_REPO_URL%/}"
# Both fluss and paimon name their Flink artifacts after the Flink minor version,
# so deriving it from the fluss artifact keeps the paimon jar in step with the base
# image whenever the artifact is overridden.
FLINK_MINOR_VERSION="${FLUSS_FLINK_CONNECTOR_ARTIFACT##*-}"

# Prints a file's sha1 with whichever tool the host has (coreutils or BSD).
sha1_of() {
    if command -v sha1sum >/dev/null 2>&1; then
        sha1sum "$1" | cut -d' ' -f1
    else
        shasum -a 1 "$1" | cut -d' ' -f1
    fi
}

# Resolves one maven artifact into a directory. The local repository is looked
# in first, so a machine that has built Doris needs no network for what that
# build already fetched; then the download cache; and only then the repository
# the version belongs to, checked against the .sha1 the repository publishes so
# that a truncated download cannot poison the cache.
#
# A release version is served under its own directory (1.0.0/): from central,
# except that org/apache/fluss releases come from FLUSS_MAVEN_REPO_URL -- the
# staging repository while 1.0.0 is a release candidate, central afterwards.
# A timestamped snapshot version (1.0-20260901.094454-3) is stored and served
# under its base version's directory (1.0-SNAPSHOT/) -- in the snapshots
# repository and in the local repository alike -- so the directory is derived
# from the version, and it comes from the apache snapshots repository whatever
# the group, so that moving fluss back to a snapshot is an edit of
# fluss.env.tpl alone.
resolve_maven_artifact() {
    local group_path="$1" artifact="$2" version="$3" extension="$4" dest_dir="$5"
    local file="${artifact}-${version}.${extension}"
    local dir_version="${version}" repo="${MAVEN_CENTRAL_URL}"
    if [[ "${group_path}" == "org/apache/fluss" ]]; then
        repo="${FLUSS_MAVEN_REPO_URL}"
    fi
    if [[ "${version}" =~ ^(.+)-[0-9]{8}\.[0-9]{6}-[0-9]+$ ]]; then
        dir_version="${BASH_REMATCH[1]}-SNAPSHOT"
        repo="${MAVEN_SNAPSHOTS_URL}"
    fi
    local local_path="${MAVEN_REPO}/${group_path}/${artifact}/${dir_version}/${file}"
    local cached="${FLUSS_ARTIFACT_CACHE}/${file}"
    local url="${repo}/${group_path}/${artifact}/${dir_version}/${file}"
    local expected actual

    mkdir -p "${dest_dir}"
    if [[ -f "${local_path}" ]]; then
        cp "${local_path}" "${dest_dir}/"
        echo "  ${file} (from ${MAVEN_REPO})"
        return 0
    fi
    if [[ -f "${cached}" ]]; then
        cp "${cached}" "${dest_dir}/"
        echo "  ${file} (from ${FLUSS_ARTIFACT_CACHE})"
        return 0
    fi

    echo "  ${file} (downloading ${url})"
    mkdir -p "${FLUSS_ARTIFACT_CACHE}"
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
    cp "${cached}" "${dest_dir}/"
}

if [[ -z "${DOCKER_USE_SUDO+x}" ]]; then
    if [[ "$(uname -s)" == "Darwin" ]]; then
        DOCKER_USE_SUDO=0
    else
        DOCKER_USE_SUDO=1
    fi
fi

docker_cli() {
    if [[ "${DOCKER_USE_SUDO}" -eq 1 ]]; then
        sudo docker "$@"
    else
        docker "$@"
    fi
}

image_exists() {
    docker_cli image inspect "$1" >/dev/null 2>&1
}

# Reuse is decided per image, because the two are built from different inputs:
# the server image from fluss-dist, the flink image from the connector, paimon
# and the tiering jar. Rebuilding one to pick up a change in the other wastes
# minutes, and -- since the base images have to be pulled -- fails outright on a
# machine that has the artifacts cached but cannot reach a registry.
should_build() {
    local image="$1"
    if [[ "${FLUSS_DOCKER_REUSE_IMAGES}" == "1" ]] && image_exists "${image}"; then
        echo "Reusing existing image ${image}"
        return 1
    fi
    return 0
}

BUILD_SERVER=0
BUILD_FLINK=0
should_build "${FLUSS_SERVER_IMAGE}" && BUILD_SERVER=1
should_build "${FLUSS_FLINK_IMAGE}" && BUILD_FLINK=1

if ((BUILD_SERVER == 0 && BUILD_FLINK == 0)); then
    exit 0
fi

BUILD_CONTEXT="$(mktemp -d)"
trap 'rm -rf "${BUILD_CONTEXT}"' EXIT

if ((BUILD_SERVER == 1)); then
    echo "Building ${FLUSS_SERVER_IMAGE} from fluss-dist ${FLUSS_VERSION}"
    mkdir -p "${BUILD_CONTEXT}/server/build-target"
    # The distribution tarball unpacks to a single fluss-<base version>/
    # directory, which the Dockerfile expects as build-target/.
    resolve_maven_artifact "org/apache/fluss" "fluss-dist" \
        "${FLUSS_VERSION}" "tgz" "${BUILD_CONTEXT}"
    tar -xzf "${BUILD_CONTEXT}/fluss-dist-${FLUSS_VERSION}.tgz" \
        -C "${BUILD_CONTEXT}/server/build-target" --strip-components=1
    rm -f "${BUILD_CONTEXT}/fluss-dist-${FLUSS_VERSION}.tgz"
    # The S3 FileIO the coordinator needs to open the warehouse. fluss-dist's
    # plugins/paimon already carries fluss-lake-paimon, paimon-bundle and a
    # shaded hadoop, but paimon keeps each filesystem implementation in a jar of
    # its own and loads it by ServiceLoader, so an s3:// warehouse is
    # unreachable -- at CREATE TABLE time, from the coordinator -- without this.
    resolve_maven_artifact "org/apache/paimon" "paimon-s3" \
        "${FLUSS_PAIMON_VERSION}" "jar" "${BUILD_CONTEXT}/server/build-target/plugins/paimon"
    cp "${SCRIPT_DIR}/server/Dockerfile" "${SCRIPT_DIR}/server/docker-entrypoint.sh" \
        "${BUILD_CONTEXT}/server/"
    docker_cli build -t "${FLUSS_SERVER_IMAGE}" "${BUILD_CONTEXT}/server"
fi

if ((BUILD_FLINK == 0)); then
    exit 0
fi

echo "Building ${FLUSS_FLINK_IMAGE} from ${FLINK_BASE_IMAGE}"
mkdir -p "${BUILD_CONTEXT}/flink/lib" "${BUILD_CONTEXT}/flink/opt"
resolve_maven_artifact "org/apache/fluss" "${FLUSS_FLINK_CONNECTOR_ARTIFACT}" \
    "${FLUSS_VERSION}" "jar" "${BUILD_CONTEXT}/flink/lib"
# Paimon runtime for the tiering job. fluss-lake-paimon is only the fluss->paimon
# writer: it carries no paimon of its own, so paimon-flink (which bundles paimon
# core) has to sit next to it, and paimon in turn builds every CatalogContext
# around a hadoop Configuration -- even a directory warehouse still needs hadoop
# present. Same four jars upstream's quickstart image activates for paimon;
# paimon-s3 is what reaches the warehouse, both for the tiering job and for the
# row counts init waits on.
resolve_maven_artifact "org/apache/fluss" "fluss-lake-paimon" \
    "${FLUSS_VERSION}" "jar" "${BUILD_CONTEXT}/flink/lib"
resolve_maven_artifact "org/apache/paimon" "paimon-flink-${FLINK_MINOR_VERSION}" \
    "${FLUSS_PAIMON_VERSION}" "jar" "${BUILD_CONTEXT}/flink/lib"
resolve_maven_artifact "org/apache/paimon" "paimon-s3" \
    "${FLUSS_PAIMON_VERSION}" "jar" "${BUILD_CONTEXT}/flink/lib"
resolve_maven_artifact "io/trino/hadoop" "hadoop-apache" \
    "${FLUSS_HADOOP_APACHE_VERSION}" "jar" "${BUILD_CONTEXT}/flink/lib"
# The lake half of the environment: the tiering job that moves fluss data into
# paimon. Submitted with `flink run`, so it goes to opt/, not lib/.
resolve_maven_artifact "org/apache/fluss" "fluss-flink-tiering" \
    "${FLUSS_VERSION}" "jar" "${BUILD_CONTEXT}/flink/opt"
cp "${SCRIPT_DIR}/flink/Dockerfile" "${BUILD_CONTEXT}/flink/Dockerfile"
docker_cli build --build-arg "FLINK_BASE_IMAGE=${FLINK_BASE_IMAGE}" \
    -t "${FLUSS_FLINK_IMAGE}" "${BUILD_CONTEXT}/flink"

echo "Built ${FLUSS_FLINK_IMAGE}"
