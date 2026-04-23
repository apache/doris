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

docker_container_state_cmd() {
    local container_name="$1"
    sudo docker inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${container_name}"
}

docker_container_env_cmd() {
    local container_name="$1"
    sudo docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "${container_name}"
}

docker_container_ready() {
    local container_name="$1"
    local state=""

    state="$(docker_container_state_cmd "${container_name}" 2>/dev/null)" || return 1
    [[ "${state}" == "running|healthy" || "${state}" == "running|none" ]]
}

docker_containers_ready() {
    local container_name=""
    for container_name in "$@"; do
        docker_container_ready "${container_name}" || return 1
    done
}

docker_hive_container_name() {
    local container_uid="$1"
    local service_name="$2"
    echo "${container_uid}${service_name}"
}

docker_hive_stack_healthy() {
    local container_uid="$1"
    local hive_version="$2"
    local containers=()

    case "${hive_version}" in
    hive2)
        containers=(
            "$(docker_hive_container_name "${container_uid}" "hadoop2-namenode")"
            "$(docker_hive_container_name "${container_uid}" "hadoop2-datanode")"
            "$(docker_hive_container_name "${container_uid}" "hive2-server")"
            "$(docker_hive_container_name "${container_uid}" "hive2-metastore")"
            "$(docker_hive_container_name "${container_uid}" "hive2-metastore-postgresql")"
        )
        ;;
    hive3)
        containers=(
            "$(docker_hive_container_name "${container_uid}" "hadoop3-namenode")"
            "$(docker_hive_container_name "${container_uid}" "hadoop3-datanode")"
            "$(docker_hive_container_name "${container_uid}" "hive3-server")"
            "$(docker_hive_container_name "${container_uid}" "hive3-metastore")"
            "$(docker_hive_container_name "${container_uid}" "hive3-metastore-postgresql")"
        )
        ;;
    *)
        echo "Unsupported hive version: ${hive_version}" >&2
        return 1
        ;;
    esac

    docker_containers_ready "${containers[@]}"
}
