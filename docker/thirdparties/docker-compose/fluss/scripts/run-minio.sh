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
# Entrypoint of the fluss minio container: starts the server and creates the
# lake bucket beside it, writing a marker once the bucket is there.
#
# The marker is what the compose healthcheck gates on, and it is not the same
# condition as "the server answers". The fluss coordinator writes into this
# bucket the moment the first datalake-enabled table is created, which is the
# first thing the fixtures do; a bucket that appears a second later turns that
# into an error whose text is about a missing bucket rather than about anything
# the environment did wrong.
################################################################

set -eo pipefail

READY_DIR=/tmp/fluss-minio
CONTROL_DIR=/tmp/fluss-minio-control
ALIAS=lake
WAIT_SECONDS=120

rm -rf "${READY_DIR}"
mkdir -p "${READY_DIR}"
rm -f "${CONTROL_DIR}/CLEANUP_REQUEST" "${CONTROL_DIR}/CLEANUP_DONE" \
    "${CONTROL_DIR}/CLEANUP_FAILED"
mkdir -p "${CONTROL_DIR}"
chmod 0777 "${CONTROL_DIR}"

process_cleanup_requests() {
    while :; do
        if [[ ! -f "${CONTROL_DIR}/CLEANUP_REQUEST" ]]; then
            sleep 1
            continue
        fi

        local target
        target="$(cat "${CONTROL_DIR}/CLEANUP_REQUEST")"
        rm -f "${CONTROL_DIR}/CLEANUP_REQUEST" "${CONTROL_DIR}/CLEANUP_DONE" \
            "${CONTROL_DIR}/CLEANUP_FAILED"
        # The sql-client is allowed to delete one database prefix in this stack's
        # own bucket, never a bucket/root or an arbitrary mc alias target.
        if [[ "${target}" != "${FLUSS_LAKE_S3_BUCKET}/"* \
                || "${target}" == *".."* \
                || "${target}" == */ ]]; then
            echo "refusing unsafe minio cleanup target: ${target}" \
                >"${CONTROL_DIR}/CLEANUP_FAILED"
            continue
        fi

        echo "Removing the paimon side of the previous attempt: s3://${target}"
        if mc rm --recursive --force "${ALIAS}/${target}"; then
            touch "${CONTROL_DIR}/CLEANUP_DONE"
        else
            echo "mc failed to remove s3://${target}" >"${CONTROL_DIR}/CLEANUP_FAILED"
        fi
    done
}

create_bucket() {
    local waited=0
    # `mc alias set` verifies the connection as it stores it, so the retry is on
    # this rather than on a separate ping: reaching the server and being able to
    # authenticate against it are both preconditions for creating the bucket,
    # and this asks for both at once.
    until mc alias set "${ALIAS}" http://127.0.0.1:9000 \
            "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
        if ((waited >= WAIT_SECONDS)); then
            echo "ERROR: minio did not accept a connection within ${WAIT_SECONDS}s" >&2
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done

    # -p so that a restarted container finds its bucket already there instead of
    # failing on it.
    mc mb -p "${ALIAS}/${FLUSS_LAKE_S3_BUCKET}"
    touch "${READY_DIR}/READY"
    echo "minio ready: bucket ${FLUSS_LAKE_S3_BUCKET} exists"
    process_cleanup_requests
}

create_bucket &

exec /bin/docker-entrypoint.sh server /data --console-address ':9001'
