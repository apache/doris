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

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DORIS_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"

: "${S3_EXPRESS_BUCKET:?Set S3_EXPRESS_BUCKET to an existing directory bucket}"
: "${S3_EXPRESS_REGION:?Set S3_EXPRESS_REGION to the bucket Region}"
: "${S3_EXPRESS_PREFIX:?Set S3_EXPRESS_PREFIX to a new dedicated test prefix}"

if ! command -v aws >/dev/null 2>&1; then
    echo "AWS CLI v2 is required" >&2
    exit 1
fi

if [[ ! "${S3_EXPRESS_BUCKET}" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?--[a-z0-9-]+-az[0-9]+--x-s3$ ]]; then
    echo "S3_EXPRESS_BUCKET must be a complete S3 Express directory bucket name" >&2
    exit 1
fi

if [[ ! "${S3_EXPRESS_REGION}" =~ ^[a-z0-9-]+$ ]]; then
    echo "S3_EXPRESS_REGION contains unsupported characters" >&2
    exit 1
fi

if [[ ! "${S3_EXPRESS_PREFIX}" =~ ^[A-Za-z0-9._/-]+$ \
        || "${S3_EXPRESS_PREFIX}" == /* \
        || "${S3_EXPRESS_PREFIX}" == */ ]]; then
    echo "S3_EXPRESS_PREFIX must be a non-empty key prefix without leading or trailing slash" >&2
    exit 1
fi

TVF_DATA="${DORIS_ROOT}/regression-test/data/external_table_p0/tvf"
AWS_ARGS=(--bucket "${S3_EXPRESS_BUCKET}" --region "${S3_EXPRESS_REGION}")

existing_key_count="$(aws s3api list-objects-v2 "${AWS_ARGS[@]}" \
    --prefix "${S3_EXPRESS_PREFIX}/" --max-keys 1 --no-paginate --query 'KeyCount' --output text)"
if [[ "${existing_key_count}" != "0" ]]; then
    echo "S3_EXPRESS_PREFIX must be empty; this script never deletes existing objects" >&2
    exit 1
fi

put_object() {
    local source_file="$1"
    local object_key="$2"
    aws s3api put-object "${AWS_ARGS[@]}" --key "${object_key}" --body "${source_file}" >/dev/null
}

put_object "${TVF_DATA}/hdfs_data_1.txt" "${S3_EXPRESS_PREFIX}/csv/data_1.csv"
put_object "${TVF_DATA}/hdfs_data_2.txt" "${S3_EXPRESS_PREFIX}/csv/data_2.csv"
put_object "${TVF_DATA}/hdfs_data_3.txt" "${S3_EXPRESS_PREFIX}/csv/data_3.csv"
put_object "${TVF_DATA}/hdfs_data_3.txt" "${S3_EXPRESS_PREFIX}/csv/ignored.csv"
put_object "${TVF_DATA}/unsigned_integers_1.parquet" \
    "${S3_EXPRESS_PREFIX}/parquet/unsigned_integers_1.parquet"
put_object "${TVF_DATA}/t.orc" "${S3_EXPRESS_PREFIX}/orc/t.orc"

pagination_data_dir="$(mktemp -d)"
cleanup_pagination_data() {
    rm -r -- "${pagination_data_dir}"
}
trap cleanup_pagination_data EXIT

for object_index in $(seq 0 1000); do
    printf -v object_name 'part-%04d.csv' "${object_index}"
    printf '1,1' >"${pagination_data_dir}/${object_name}"
done
aws s3 cp "${pagination_data_dir}/" \
    "s3://${S3_EXPRESS_BUCKET}/${S3_EXPRESS_PREFIX}/pagination/" \
    --recursive --region "${S3_EXPRESS_REGION}" --only-show-errors

echo "Prepared S3 Express regression data under s3://${S3_EXPRESS_BUCKET}/${S3_EXPRESS_PREFIX}/"
