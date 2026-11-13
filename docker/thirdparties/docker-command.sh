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

configure_docker_access() {
    local rootless_runtime_dir
    local rootless_socket
    local security_options

    rootless_runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
    rootless_socket="${rootless_runtime_dir}/docker.sock"

    # An explicit endpoint or context takes precedence over socket discovery.
    if [[ "${HOST_OS}" != "Darwin" && -z "${DOCKER_HOST+x}" \
        && -z "${DOCKER_CONTEXT:-}" && -S "${rootless_socket}" ]]; then
        export DOCKER_HOST="unix://${rootless_socket}"
    fi

    if ! security_options="$(docker info --format '{{json .SecurityOptions}}')"; then
        echo "Unable to access Docker as the current user; start the user's rootless daemon or select its endpoint" >&2
        return 1
    fi
    # Docker Desktop on macOS runs its daemon in a VM. Linux requires the
    # user's rootless daemon; an accessible system daemon is not accepted.
    if [[ "${HOST_OS}" != "Darwin" && "${security_options}" != *'"name=rootless"'* ]]; then
        echo "This launcher requires rootless Docker on Linux; the selected daemon is rootful" >&2
        return 1
    fi
}

require_rootless_host_network() {
    local server_version
    local major
    local minor

    if [[ "${HOST_OS}" == "Darwin" ]]; then
        return 0
    fi
    server_version="$(docker_cli info --format '{{.ServerVersion}}')" || return
    major="${server_version%%.*}"
    minor="${server_version#*.}"
    minor="${minor%%.*}"
    # Older rootless host networks cannot reach ports published on the real
    # host, which these fixtures use for PostgreSQL and HDFS RPC.
    if ((major < 29 || (major == 29 && minor < 5))); then
        echo "Rootless host networking requires Docker Engine 29.5 or newer (found ${server_version})" >&2
        return 1
    fi
}

rootless_fixture_cmd() {
    local fixture_dir="$1"
    shift

    # Stay in the daemon's user namespace when images have chowned bind mounts
    # to subordinate UIDs. Mount only the fixture directory being operated on.
    docker_cli run --rm --network none --user 0:0 \
        --security-opt no-new-privileges \
        --mount "type=bind,source=${fixture_dir},target=/fixture" \
        "${THIRDPARTY_FILESYSTEM_IMAGE:-alpine:3.20}" "$@"
}

reset_rootless_data_dir() {
    local data_dir="$1"

    mkdir -p "${data_dir}"
    rootless_fixture_cmd "${data_dir}" sh -ec '
            find /fixture -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
            chown 0:0 /fixture
            chmod 755 /fixture
        '
}

chmod_fixture_dir() {
    local mode="$1"
    local fixture_dir="$2"
    rootless_fixture_cmd "${fixture_dir}" chmod -R "${mode}" /fixture
}
