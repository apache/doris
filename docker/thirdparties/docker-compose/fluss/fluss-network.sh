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

resolve_fluss_host_ip() {
    local address="${FLUSS_HOST_IP:-${IP_HOST:-}}"

    # Unlike host-network Hive, the Fluss servers and Flink clients live in
    # separate bridge containers. Their advertised endpoint cannot be loopback.
    case "${address}" in
    ""|localhost|localhost.|127.*|::1|"[::1]"|::|"[::]"|0.0.0.0)
        if [[ -n "${FLUSS_HOST_IP:-}" ]]; then
            echo "FLUSS_HOST_IP must be reachable from both the host and bridge containers; loopback and wildcard addresses cannot be advertised" >&2
            return 1
        fi
        address=""
        if [[ "${HOST_OS}" != "Darwin" ]]; then
            if command -v ip >/dev/null 2>&1; then
                # Reading the route does not send traffic to this address.
                address=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1; i<NF; i++) if ($i == "src") {print $(i+1); exit}}') || address=""
            fi
            if [[ -z "${address}" ]]; then
                address=$(hostname -I 2>/dev/null | awk '{for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+\./ && $i !~ /^127\./) {print $i; exit}}') || address=""
            fi
        fi
        ;;
    esac

    case "${address}" in
    ""|localhost|localhost.|127.*|::1|"[::1]"|::|"[::]"|0.0.0.0)
        echo "Cannot determine a Fluss host address; set FLUSS_HOST_IP to an address reachable from both the host and bridge containers" >&2
        return 1
        ;;
    esac
    printf '%s\n' "${address}"
}
