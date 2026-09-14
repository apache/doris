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
#
# One-shot: create the cloud instance on the meta-service, backed by an
# S3-compatible object store, then exit. Safe to run again -- an instance that
# already exists is left alone.
#
#   MS_ENDPOINT   host:port of the meta-service
#   INSTANCE_ID   numeric; the FEs use the same value as their cluster_id
#   S3_ENDPOINT   host:port of the object store (MinIO in the compose file)
#   S3_BUCKET, S3_AK, S3_SK, S3_REGION, S3_PREFIX, S3_PROVIDER (S3 | OSS | COS | OBS | BOS | GCP | AZURE)
#   S3_PATH_STYLE true for MinIO, false for the public clouds
#   VAULT_MODE    true (default) creates a storage-vault instance, false a legacy obj_info one

set -Eeuo pipefail
CI_HOME="${CI_HOME:-/opt/doris-ci}"
# shellcheck source=lib.sh
source "${CI_HOME}/lib.sh"

: "${MS_ENDPOINT:?}" "${INSTANCE_ID:?}" "${S3_ENDPOINT:?}" "${S3_BUCKET:?}" "${S3_AK:?}" "${S3_SK:?}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_PREFIX="${S3_PREFIX:-doris}"
S3_PROVIDER="${S3_PROVIDER:-S3}"
S3_PATH_STYLE="${S3_PATH_STYLE:-true}"
VAULT_MODE="${VAULT_MODE:-true}"
MS_TOKEN="${MS_TOKEN:-greedisgood9999}"
INSTANCE_NAME="${INSTANCE_NAME:-doris_${INSTANCE_ID}}"

ms_url() { echo "http://${MS_ENDPOINT}/MetaService/http/$1?token=${MS_TOKEN}"; }
ms_up()  { curl -fsS --max-time 4 "http://${MS_ENDPOINT}/health" >/dev/null 2>&1; }

wait_until "${START_TIMEOUT}" "meta-service at ${MS_ENDPOINT}" ms_up

obj_info=$(printf '{"ak":"%s","sk":"%s","bucket":"%s","endpoint":"%s","external_endpoint":"%s","prefix":"%s","region":"%s","provider":"%s","use_path_style":%s}' \
    "${S3_AK}" "${S3_SK}" "${S3_BUCKET}" "${S3_ENDPOINT}" "${S3_ENDPOINT}" "${S3_PREFIX}" "${S3_REGION}" "${S3_PROVIDER}" "${S3_PATH_STYLE}")
if [[ "${VAULT_MODE}" == true ]]; then
    body=$(printf '{"instance_id":"%s","name":"%s","user_id":"doris","vault":{"obj_info":%s}}' "${INSTANCE_ID}" "${INSTANCE_NAME}" "${obj_info}")
else
    body=$(printf '{"instance_id":"%s","name":"%s","user_id":"doris","obj_info":%s}' "${INSTANCE_ID}" "${INSTANCE_NAME}" "${obj_info}")
fi

info "creating instance ${INSTANCE_ID} on ${S3_PROVIDER} ${S3_ENDPOINT}/${S3_BUCKET}/${S3_PREFIX} (vault=${VAULT_MODE}, path_style=${S3_PATH_STYLE})"
out=$(curl -sS --max-time 30 "$(ms_url create_instance)" -d "${body}") || die "create_instance request failed"
code=$(sed -nE 's/.*"code"[[:space:]]*:[[:space:]]*"([A-Z_]+)".*/\1/p' <<<"$(tr -d '\n' <<<"${out}")")
case "${code}" in
    OK)              info "instance ${INSTANCE_ID} created" ;;
    ALREADY_EXISTED) info "instance ${INSTANCE_ID} already exists, leaving it alone" ;;
    *)               die "create_instance failed: ${out}" ;;
esac
