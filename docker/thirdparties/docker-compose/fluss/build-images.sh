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
#   FLUSS_DOCKER_REUSE_IMAGES  1 = skip the build when both tags already exist
################################################################

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Image tags live in fluss.env.tpl so the compose file and this script cannot
# drift apart. The template's other entries reference variables that are empty
# here; only the two image tags are read.
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/fluss.env.tpl"

FLUSS_VERSION="${FLUSS_VERSION:-1.0-SNAPSHOT}"
FLINK_BASE_IMAGE="${FLINK_BASE_IMAGE:-flink:1.20.0-scala_2.12-java17}"
FLUSS_FLINK_CONNECTOR_MODULE="${FLUSS_FLINK_CONNECTOR_MODULE:-fluss-flink-1.20}"

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

if [[ "${FLUSS_DOCKER_REUSE_IMAGES}" == "1" ]] &&
    image_exists "${FLUSS_SERVER_IMAGE}" && image_exists "${FLUSS_FLINK_IMAGE}"; then
    echo "Reusing existing images ${FLUSS_SERVER_IMAGE} and ${FLUSS_FLINK_IMAGE}"
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

if [[ ! -d "${DIST_DIR}" || ! -f "${CONNECTOR_JAR}" ]]; then
    echo "ERROR: fluss build output is missing:" >&2
    [[ -d "${DIST_DIR}" ]] || echo "         ${DIST_DIR}" >&2
    [[ -f "${CONNECTOR_JAR}" ]] || echo "         ${CONNECTOR_JAR}" >&2
    echo "       Build them with:" >&2
    echo "         mvn -f ${FLUSS_SOURCE_DIR}/pom.xml -pl fluss-dist,fluss-flink/${FLUSS_FLINK_CONNECTOR_MODULE} -am package -DskipTests" >&2
    exit 1
fi

BUILD_CONTEXT="$(mktemp -d)"
trap 'rm -rf "${BUILD_CONTEXT}"' EXIT

echo "Building ${FLUSS_SERVER_IMAGE} from ${DIST_DIR}"
mkdir -p "${BUILD_CONTEXT}/server"
cp -r "${DIST_DIR}" "${BUILD_CONTEXT}/server/build-target"
cp "${FLUSS_SOURCE_DIR}/docker/fluss/Dockerfile" "${BUILD_CONTEXT}/server/Dockerfile"
cp "${FLUSS_SOURCE_DIR}/docker/fluss/docker-entrypoint.sh" "${BUILD_CONTEXT}/server/docker-entrypoint.sh"
docker_cli build -t "${FLUSS_SERVER_IMAGE}" "${BUILD_CONTEXT}/server"

echo "Building ${FLUSS_FLINK_IMAGE} from ${FLINK_BASE_IMAGE} + $(basename "${CONNECTOR_JAR}")"
mkdir -p "${BUILD_CONTEXT}/flink/lib"
cp "${CONNECTOR_JAR}" "${BUILD_CONTEXT}/flink/lib/"
cp "${SCRIPT_DIR}/flink/Dockerfile" "${BUILD_CONTEXT}/flink/Dockerfile"
docker_cli build --build-arg "FLINK_BASE_IMAGE=${FLINK_BASE_IMAGE}" \
    -t "${FLUSS_FLINK_IMAGE}" "${BUILD_CONTEXT}/flink"

echo "Built ${FLUSS_SERVER_IMAGE} and ${FLUSS_FLINK_IMAGE}"
