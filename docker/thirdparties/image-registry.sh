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

# This file has no initialization side effects; standalone helpers can use the
# resolver without loading the launcher or contacting a Docker daemon.

declare -a THIRDPARTY_IMAGE_KEYS=() THIRDPARTY_SELECTED_COMPONENTS=()
declare -A THIRDPARTY_IMAGE_SOURCES=() THIRDPARTY_IMAGE_TARGETS=()
declare -A THIRDPARTY_IMAGE_KINDS=() THIRDPARTY_IMAGE_COMPONENTS=()

thirdparty_image_error() {
    echo "ERROR: thirdparty images: $*" >&2
    return 1
}

thirdparty_registry_name() {
    case "$1" in
    docker.io|index.docker.io|registry-1.docker.io) echo docker.io ;;
    *) printf '%s\n' "$1" ;;
    esac
}

thirdparty_validate_image_config() {
    local entry source prefix
    local -a entries
    local -A seen=()
    case "${THIRDPARTY_IMAGE_MODE:-native}" in
    native) return 0 ;;
    mapped) ;;
    *) thirdparty_image_error 'THIRDPARTY_IMAGE_MODE must be native or mapped'; return 1 ;;
    esac
    if [[ ! "${THIRDPARTY_IMAGE_REGISTRY:-}" =~ ^[a-z0-9][a-z0-9.-]*(:[0-9]+)?$ ]] \
        || [[ "${THIRDPARTY_IMAGE_REGISTRY}" != *.* && "${THIRDPARTY_IMAGE_REGISTRY}" != *:* \
            && "${THIRDPARTY_IMAGE_REGISTRY}" != localhost ]]; then
        thirdparty_image_error 'THIRDPARTY_IMAGE_REGISTRY must be a registry host[:port], without a scheme or path'
        return 1
    fi
    if [[ "${THIRDPARTY_IMAGE_REGISTRY_MAP:-}" == *[[:space:]]* ]]; then
        thirdparty_image_error 'registry mappings must not contain whitespace'
        return 1
    fi
    if [[ "${THIRDPARTY_IMAGE_REGISTRY_MAP:-}" == ,* || "${THIRDPARTY_IMAGE_REGISTRY_MAP:-}" == *, \
        || "${THIRDPARTY_IMAGE_REGISTRY_MAP:-}" == *,,* ]]; then
        thirdparty_image_error 'empty registry mapping entry'
        return 1
    fi
    IFS=',' read -r -a entries <<<"${THIRDPARTY_IMAGE_REGISTRY_MAP:-}"
    for entry in "${entries[@]}"; do
        source="${entry%%=*}"
        prefix="${entry#*=}"
        if [[ "${entry}" != *=* || ! "${source}" =~ ^[a-z0-9][a-z0-9.-]*(:[0-9]+)?$ \
            || ! "${prefix}" =~ ^[a-z0-9]+([._-][a-z0-9]+)*(/[a-z0-9]+([._-][a-z0-9]+)*)*$ ]]; then
            thirdparty_image_error 'registry mappings must be source=project[/prefix] entries'
            return 1
        fi
        source="$(thirdparty_registry_name "${source}")"
        if [[ -n "${seen[${source}]:-}" ]]; then
            thirdparty_image_error "duplicate registry mapping for ${source}"
            return 1
        fi
        seen["${source}"]=1
    done
}

thirdparty_resolve_image() {
    local reference="$1"
    local registry repository first entry source prefix
    local -a entries
    thirdparty_validate_image_config || return
    if [[ -z "${reference}" || "${reference}" == *://* || "${reference}" == *[[:space:]]* ]]; then
        thirdparty_image_error 'image must be a nonempty reference without a scheme or whitespace'
        return 1
    fi
    if [[ "${THIRDPARTY_IMAGE_MODE:-native}" == native ]]; then
        printf '%s\n' "${reference}"
        return
    fi
    # A full target reference is an explicit override, including private projects.
    if [[ "${reference}" == "${THIRDPARTY_IMAGE_REGISTRY}/"* ]]; then
        printf '%s\n' "${reference}"
        return
    fi
    registry=docker.io
    repository="${reference}"
    first="${reference%%/*}"
    if [[ "${reference}" == */* && ( "${first}" == *.* || "${first}" == *:* || "${first}" == localhost ) ]]; then
        registry="$(thirdparty_registry_name "${first}")"
        repository="${reference#*/}"
    fi
    if [[ "${registry}" == docker.io && "${repository}" != */* ]]; then
        repository="library/${repository}"
    fi
    IFS=',' read -r -a entries <<<"${THIRDPARTY_IMAGE_REGISTRY_MAP:-}"
    for entry in "${entries[@]}"; do
        source="$(thirdparty_registry_name "${entry%%=*}")"
        if [[ "${source}" == "${registry}" ]]; then
            prefix="${entry#*=}"
            printf '%s/%s/%s\n' "${THIRDPARTY_IMAGE_REGISTRY}" "${prefix}" "${repository}"
            return
        fi
    done
    thirdparty_image_error "no mapping for registry ${registry} (image ${reference})"
}

# Called by image-catalog.env. Resolve only requested components and their
# helper images, so an unused registry does not require configuration.
thirdparty_image() {
    local components="$1" variable="$2" default="$3" kind="$4"
    local component selected=0 source target
    for component in "${THIRDPARTY_SELECTED_COMPONENTS[@]}"; do
        if [[ ",${components}," == *",${component},"* ]]; then
            selected=1
            break
        fi
    done
    [[ "${selected}" -eq 1 ]] || return 0
    source="${!variable:-${default}}"
    target="${source}"
    if [[ "${kind}" == remote ]]; then
        target="$(thirdparty_resolve_image "${source}")" || return
    fi
    THIRDPARTY_IMAGE_KEYS+=("${variable}")
    THIRDPARTY_IMAGE_SOURCES["${variable}"]="${source}"
    THIRDPARTY_IMAGE_TARGETS["${variable}"]="${target}"
    THIRDPARTY_IMAGE_KINDS["${variable}"]="${kind}"
    THIRDPARTY_IMAGE_COMPONENTS["${variable}"]="${components}"
    export "${variable}=${target}"
}

thirdparty_images_init() {
    local component
    local thirdparty_iceberg_postgres_default=postgis/postgis:14-3.3
    if [[ "${HOST_OS:-$(uname -s)}" == Darwin ]]; then
        thirdparty_iceberg_postgres_default=postgres:14
    fi
    thirdparty_validate_image_config || return
    # This is an explicit interface shared by the launcher and standalone tools.
    # filesystem is for helpers only, not a launchable component.
    if [[ -z "$1" || "$1" == ,* || "$1" == *, || "$1" == *,,* ]]; then
        thirdparty_image_error 'empty component selection'
        return 1
    fi
    IFS=',' read -r -a THIRDPARTY_SELECTED_COMPONENTS <<<"$1"
    for component in "${THIRDPARTY_SELECTED_COMPONENTS[@]}"; do
        case "${component}" in
        mysql|pg|oracle|sqlserver|clickhouse|es|hive2|hive3|iceberg|iceberg-rest|hudi|kafka|mariadb|db2|oceanbase|kerberos|ranger|polaris|minio|fluss|mongodb|filesystem) ;;
        *) thirdparty_image_error "unknown component ${component}"; return 1 ;;
        esac
    done
    THIRDPARTY_IMAGE_KEYS=()
    THIRDPARTY_IMAGE_SOURCES=() THIRDPARTY_IMAGE_TARGETS=()
    THIRDPARTY_IMAGE_KINDS=() THIRDPARTY_IMAGE_COMPONENTS=()
    # shellcheck source=image-catalog.env
    . "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/image-catalog.env" || return
}

thirdparty_images_list() {
    local key
    printf 'COMPONENTS\tVARIABLE\tKIND\tSOURCE\tTARGET\n'
    for key in "${THIRDPARTY_IMAGE_KEYS[@]}"; do
        printf '%s\t%s\t%s\t%s\t%s\n' "${THIRDPARTY_IMAGE_COMPONENTS[${key}]}" "${key}" \
            "${THIRDPARTY_IMAGE_KINDS[${key}]}" "${THIRDPARTY_IMAGE_SOURCES[${key}]}" "${THIRDPARTY_IMAGE_TARGETS[${key}]}"
    done
}

thirdparty_images_pull() {
    local key target
    local -A pulled=()
    for key in "${THIRDPARTY_IMAGE_KEYS[@]}"; do
        [[ "${THIRDPARTY_IMAGE_KINDS[${key}]}" == remote ]] || continue
        target="${THIRDPARTY_IMAGE_TARGETS[${key}]}"
        [[ -z "${pulled[${target}]:-}" ]] || continue
        # Match Compose's default missing-image policy; do not refresh cached tags.
        if ! docker_cli image inspect "${target}" >/dev/null 2>&1; then
            if ! docker_cli pull "${target}"; then
                thirdparty_image_error "pull failed: ${THIRDPARTY_IMAGE_COMPONENTS[${key}]} ${key} ${target}"
                return 1
            fi
        fi
        pulled["${target}"]=1
    done
}
