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
# Fluss 1.0 is not released, so neither image can be pulled: both are built
# from a local fluss source checkout that has already been packaged. Once
# fluss 1.0 ships, this script is replaced by pulling the official images.
#
# Required:
#   FLUSS_SOURCE_DIR   path to a built fluss checkout
# Optional:
#   FLUSS_VERSION      fluss version in that checkout (default 1.0-SNAPSHOT)
#   FLINK_BASE_IMAGE   base Flink image (default flink:1.20.0-scala_2.12-java17)
#   FLUSS_FLINK_CONNECTOR_MODULE  fluss connector module matching the base image
#   FLUSS_DOCKER_REUSE_IMAGES  1 = skip an image whose tag already exists
#                              (decided per image; delete a tag to rebuild just it)
#   MAVEN_REPO         local maven repository (default ~/.m2/repository)
################################################################

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Image tags and the paimon version live in fluss.env.tpl so the compose file and
# this script cannot drift apart. The template's other entries reference
# variables that are empty here; only those three literals are read.
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/fluss.env.tpl"

FLUSS_VERSION="${FLUSS_VERSION:-1.0-SNAPSHOT}"
FLINK_BASE_IMAGE="${FLINK_BASE_IMAGE:-flink:1.20.0-scala_2.12-java17}"
FLUSS_FLINK_CONNECTOR_MODULE="${FLUSS_FLINK_CONNECTOR_MODULE:-fluss-flink-1.20}"
MAVEN_REPO="${MAVEN_REPO:-${HOME}/.m2/repository}"
# Both fluss and paimon name their Flink artifacts after the Flink minor version,
# so deriving it from the fluss module keeps the paimon jar in step with the base
# image whenever the module is overridden.
FLINK_MINOR_VERSION="${FLUSS_FLINK_CONNECTOR_MODULE##*-}"

# Resolves a maven artifact into a directory: the local repository first, so a
# machine that has already built Doris or fluss needs no network, then central.
resolve_maven_jar() {
    local group_path="$1" artifact="$2" version="$3" dest_dir="$4"
    local jar="${artifact}-${version}.jar"
    local local_path="${MAVEN_REPO}/${group_path}/${artifact}/${version}/${jar}"

    mkdir -p "${dest_dir}"
    if [[ -f "${local_path}" ]]; then
        cp "${local_path}" "${dest_dir}/"
        echo "  ${jar} (from ${MAVEN_REPO})"
        return 0
    fi
    local url="https://repo1.maven.org/maven2/${group_path}/${artifact}/${version}/${jar}"
    echo "  ${jar} (downloading ${url})"
    if ! curl -fsSL -o "${dest_dir}/${jar}" "${url}"; then
        echo "ERROR: could not resolve ${jar} locally or from central" >&2
        return 1
    fi
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
# machine that can reach the local checkout but not a registry.
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

if [[ -z "${FLUSS_SOURCE_DIR}" ]]; then
    echo "ERROR: FLUSS_SOURCE_DIR is not set." >&2
    echo "       Clone https://github.com/apache/fluss, build it, and point" >&2
    echo "       FLUSS_SOURCE_DIR at the checkout." >&2
    exit 1
fi

DIST_DIR="${FLUSS_SOURCE_DIR}/fluss-dist/target/fluss-${FLUSS_VERSION}-bin/fluss-${FLUSS_VERSION}"
CONNECTOR_JAR="${FLUSS_SOURCE_DIR}/fluss-flink/${FLUSS_FLINK_CONNECTOR_MODULE}/target/${FLUSS_FLINK_CONNECTOR_MODULE}-${FLUSS_VERSION}.jar"
# The lake half of the environment: the tiering job that moves fluss data into
# paimon, and the fluss-side paimon writer it loads.
TIERING_JAR="${FLUSS_SOURCE_DIR}/fluss-flink/fluss-flink-tiering/target/fluss-flink-tiering-${FLUSS_VERSION}.jar"
LAKE_PAIMON_JAR="${FLUSS_SOURCE_DIR}/fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-${FLUSS_VERSION}.jar"

MISSING=()
[[ -d "${DIST_DIR}" ]] || MISSING+=("${DIST_DIR}")
for jar in "${CONNECTOR_JAR}" "${TIERING_JAR}" "${LAKE_PAIMON_JAR}"; do
    [[ -f "${jar}" ]] || MISSING+=("${jar}")
done

if ((${#MISSING[@]} > 0)); then
    echo "ERROR: fluss build output is missing:" >&2
    printf '         %s\n' "${MISSING[@]}" >&2
    echo "       Build them with:" >&2
    echo "         mvn -f ${FLUSS_SOURCE_DIR}/pom.xml -pl fluss-dist,fluss-flink/${FLUSS_FLINK_CONNECTOR_MODULE},fluss-flink/fluss-flink-tiering,fluss-lake/fluss-lake-paimon -am package -DskipTests" >&2
    exit 1
fi

BUILD_CONTEXT="$(mktemp -d)"
trap 'rm -rf "${BUILD_CONTEXT}"' EXIT

if ((BUILD_SERVER == 1)); then
    echo "Building ${FLUSS_SERVER_IMAGE} from ${DIST_DIR}"
    mkdir -p "${BUILD_CONTEXT}/server"
    cp -r "${DIST_DIR}" "${BUILD_CONTEXT}/server/build-target"
    cp "${FLUSS_SOURCE_DIR}/docker/fluss/Dockerfile" "${BUILD_CONTEXT}/server/Dockerfile"
    cp "${FLUSS_SOURCE_DIR}/docker/fluss/docker-entrypoint.sh" "${BUILD_CONTEXT}/server/docker-entrypoint.sh"
    docker_cli build -t "${FLUSS_SERVER_IMAGE}" "${BUILD_CONTEXT}/server"
fi

if ((BUILD_FLINK == 0)); then
    exit 0
fi

echo "Building ${FLUSS_FLINK_IMAGE} from ${FLINK_BASE_IMAGE}"
mkdir -p "${BUILD_CONTEXT}/flink/lib" "${BUILD_CONTEXT}/flink/opt"
cp "${CONNECTOR_JAR}" "${BUILD_CONTEXT}/flink/lib/"
echo "  $(basename "${CONNECTOR_JAR}")"
# Paimon runtime for the tiering job. fluss-lake-paimon is only the fluss->paimon
# writer: it carries no paimon of its own, so paimon-flink (which bundles paimon
# core) has to sit next to it, and paimon in turn builds every CatalogContext
# around a hadoop Configuration -- a plain directory warehouse still needs hadoop
# present. Same three jars upstream's quickstart image activates for paimon,
# minus paimon-s3: this warehouse is a bind-mounted directory.
cp "${LAKE_PAIMON_JAR}" "${BUILD_CONTEXT}/flink/lib/"
echo "  $(basename "${LAKE_PAIMON_JAR}")"
resolve_maven_jar "org/apache/paimon" "paimon-flink-${FLINK_MINOR_VERSION}" \
    "${FLUSS_PAIMON_VERSION}" "${BUILD_CONTEXT}/flink/lib"
resolve_maven_jar "io/trino/hadoop" "hadoop-apache" \
    "${FLUSS_HADOOP_APACHE_VERSION}" "${BUILD_CONTEXT}/flink/lib"
cp "${TIERING_JAR}" "${BUILD_CONTEXT}/flink/opt/"
echo "  opt/$(basename "${TIERING_JAR}")"
cp "${SCRIPT_DIR}/flink/Dockerfile" "${BUILD_CONTEXT}/flink/Dockerfile"
docker_cli build --build-arg "FLINK_BASE_IMAGE=${FLINK_BASE_IMAGE}" \
    -t "${FLUSS_FLINK_IMAGE}" "${BUILD_CONTEXT}/flink"

echo "Built ${FLUSS_FLINK_IMAGE}"
