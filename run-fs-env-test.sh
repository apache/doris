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

# Usage:
#   ./run-fs-env-test.sh s3 --s3-endpoint=... --s3-bucket=... --s3-ak=... --s3-sk=... --s3-region=...
#   ./run-fs-env-test.sh azure --azure-account=... --azure-key=... --azure-container=...
#   ./run-fs-env-test.sh cos --cos-endpoint=... --cos-region=... --cos-bucket=... --cos-ak=... --cos-sk=...
#   ./run-fs-env-test.sh oss --oss-endpoint=... --oss-region=... --oss-bucket=... --oss-ak=... --oss-sk=...
#   ./run-fs-env-test.sh obs --obs-endpoint=... --obs-region=... --obs-bucket=... --obs-ak=... --obs-sk=...
#   ./run-fs-env-test.sh hdfs --hdfs-host=... --hdfs-port=...
#   ./run-fs-env-test.sh kerberos --kdc-principal=... --kdc-keytab=... --hdfs-host=... --hdfs-port=...
#   ./run-fs-env-test.sh broker --broker-host=... --broker-port=...
#   ./run-fs-env-test.sh all     # run all env tests (requires all env vars pre-set)
#
# You can also pre-export environment variables:
#   export DORIS_FS_TEST_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
#   export DORIS_FS_TEST_S3_BUCKET=my-test-bucket
#   ...
#   ./run-fs-env-test.sh s3

set -eo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

SERVICE="${1:?Usage: $0 <s3|azure|cos|oss|obs|hdfs|kerberos|broker|all> [--key=value ...]}"
shift

# Parse --key=value arguments into env vars
for arg in "$@"; do
    case "$arg" in
        --s3-endpoint=*)  export DORIS_FS_TEST_S3_ENDPOINT="${arg#*=}" ;;
        --s3-region=*)    export DORIS_FS_TEST_S3_REGION="${arg#*=}" ;;
        --s3-bucket=*)    export DORIS_FS_TEST_S3_BUCKET="${arg#*=}" ;;
        --s3-ak=*)        export DORIS_FS_TEST_S3_AK="${arg#*=}" ;;
        --s3-sk=*)        export DORIS_FS_TEST_S3_SK="${arg#*=}" ;;
        --azure-account=*)    export DORIS_FS_TEST_AZURE_ACCOUNT="${arg#*=}" ;;
        --azure-key=*)        export DORIS_FS_TEST_AZURE_KEY="${arg#*=}" ;;
        --azure-container=*)  export DORIS_FS_TEST_AZURE_CONTAINER="${arg#*=}" ;;
        --cos-endpoint=*)  export DORIS_FS_TEST_COS_ENDPOINT="${arg#*=}" ;;
        --cos-region=*)    export DORIS_FS_TEST_COS_REGION="${arg#*=}" ;;
        --cos-bucket=*)    export DORIS_FS_TEST_COS_BUCKET="${arg#*=}" ;;
        --cos-ak=*)        export DORIS_FS_TEST_COS_AK="${arg#*=}" ;;
        --cos-sk=*)        export DORIS_FS_TEST_COS_SK="${arg#*=}" ;;
        --oss-endpoint=*)  export DORIS_FS_TEST_OSS_ENDPOINT="${arg#*=}" ;;
        --oss-region=*)    export DORIS_FS_TEST_OSS_REGION="${arg#*=}" ;;
        --oss-bucket=*)    export DORIS_FS_TEST_OSS_BUCKET="${arg#*=}" ;;
        --oss-ak=*)        export DORIS_FS_TEST_OSS_AK="${arg#*=}" ;;
        --oss-sk=*)        export DORIS_FS_TEST_OSS_SK="${arg#*=}" ;;
        --obs-endpoint=*)  export DORIS_FS_TEST_OBS_ENDPOINT="${arg#*=}" ;;
        --obs-region=*)    export DORIS_FS_TEST_OBS_REGION="${arg#*=}" ;;
        --obs-bucket=*)    export DORIS_FS_TEST_OBS_BUCKET="${arg#*=}" ;;
        --obs-ak=*)        export DORIS_FS_TEST_OBS_AK="${arg#*=}" ;;
        --obs-sk=*)        export DORIS_FS_TEST_OBS_SK="${arg#*=}" ;;
        --hdfs-host=*)    export DORIS_FS_TEST_HDFS_HOST="${arg#*=}" ;;
        --hdfs-port=*)    export DORIS_FS_TEST_HDFS_PORT="${arg#*=}" ;;
        --kdc-principal=*) export DORIS_FS_TEST_KDC_PRINCIPAL="${arg#*=}" ;;
        --kdc-keytab=*)    export DORIS_FS_TEST_KDC_KEYTAB="${arg#*=}" ;;
        --broker-host=*)  export DORIS_FS_TEST_BROKER_HOST="${arg#*=}" ;;
        --broker-port=*)  export DORIS_FS_TEST_BROKER_PORT="${arg#*=}" ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

# Map service name to JUnit 5 tag and Maven module(s)
# NOTE: Do NOT use GROUPS as variable name — it is a bash builtin (user group IDs).
case "$SERVICE" in
    s3)        TAG="s3";       MODULES="fe-filesystem/fe-filesystem-s3" ;;
    oss)       TAG="oss";      MODULES="fe-filesystem/fe-filesystem-oss" ;;
    cos)       TAG="cos";      MODULES="fe-filesystem/fe-filesystem-cos" ;;
    obs)       TAG="obs";      MODULES="fe-filesystem/fe-filesystem-obs" ;;
    azure)     TAG="azure";    MODULES="fe-filesystem/fe-filesystem-azure" ;;
    hdfs)      TAG="hdfs";     MODULES="fe-filesystem/fe-filesystem-hdfs" ;;
    kerberos)  TAG="kerberos"; MODULES="fe-filesystem/fe-filesystem-hdfs" ;;
    broker)    TAG="broker";   MODULES="fe-filesystem/fe-filesystem-broker" ;;
    all)       TAG="environment"
               MODULES="fe-filesystem/fe-filesystem-s3,fe-filesystem/fe-filesystem-oss"
               MODULES="${MODULES},fe-filesystem/fe-filesystem-cos,fe-filesystem/fe-filesystem-obs"
               MODULES="${MODULES},fe-filesystem/fe-filesystem-azure,fe-filesystem/fe-filesystem-hdfs"
               MODULES="${MODULES},fe-filesystem/fe-filesystem-broker" ;;
    *) echo "Unknown service: $SERVICE"; exit 1 ;;
esac

cd "${ROOT}/fe"
echo "Running filesystem environment tests for: $TAG (modules: $MODULES)"
mvn test -pl "${MODULES}" \
    -Dtest.excludedGroups=none \
    -Dgroups="$TAG" \
    -Dcheckstyle.skip=true \
    -DfailIfNoTests=false \
    -Dmaven.build.cache.enabled=false \
    --also-make
