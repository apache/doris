<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Filesystem Environment Tests

The environment tests (Layer 2) in this directory require real cloud storage / HDFS / Broker
services to run. They are automatically skipped in default CI builds.

## Quick Start

### Using the Helper Script

The repository root provides `run-fs-env-test.sh`, which supports running tests via command-line
arguments or pre-set environment variables:

```bash
# S3 tests
./run-fs-env-test.sh s3 \
  --s3-endpoint=https://s3.us-east-1.amazonaws.com \
  --s3-region=us-east-1 \
  --s3-bucket=my-test-bucket \
  --s3-ak=AKIAIOSFODNN7EXAMPLE \
  --s3-sk=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Azure tests
./run-fs-env-test.sh azure \
  --azure-account=myaccount \
  --azure-key=base64key== \
  --azure-container=testcontainer

# HDFS tests (Simple Auth)
./run-fs-env-test.sh hdfs \
  --hdfs-host=namenode.example.com \
  --hdfs-port=8020

# Kerberos tests
./run-fs-env-test.sh kerberos \
  --kdc-principal=hdfs/namenode@REALM \
  --kdc-keytab=/path/to/hdfs.keytab \
  --hdfs-host=namenode.example.com \
  --hdfs-port=8020

# COS (Tencent Cloud) tests
./run-fs-env-test.sh cos \
  --cos-endpoint=https://cos.ap-guangzhou.myqcloud.com \
  --cos-region=ap-guangzhou \
  --cos-bucket=my-cos-bucket \
  --cos-ak=SecretId \
  --cos-sk=SecretKey

# OSS (Alibaba Cloud) tests
./run-fs-env-test.sh oss \
  --oss-endpoint=https://oss-cn-hangzhou.aliyuncs.com \
  --oss-region=cn-hangzhou \
  --oss-bucket=my-oss-bucket \
  --oss-ak=AccessKeyId \
  --oss-sk=AccessKeySecret

# OBS (Huawei Cloud) tests
./run-fs-env-test.sh obs \
  --obs-endpoint=https://obs.cn-north-4.myhuaweicloud.com \
  --obs-region=cn-north-4 \
  --obs-bucket=my-obs-bucket \
  --obs-ak=AK \
  --obs-sk=SK

# Broker tests
./run-fs-env-test.sh broker \
  --broker-host=broker.example.com \
  --broker-port=8060

# Run all environment tests (requires all environment variables to be pre-set)
./run-fs-env-test.sh all
```

### Running Directly with Maven

You can also export environment variables first and then run tests directly with Maven:

```bash
# Export credentials
export DORIS_FS_TEST_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
export DORIS_FS_TEST_S3_REGION=us-east-1
export DORIS_FS_TEST_S3_BUCKET=my-test-bucket
export DORIS_FS_TEST_S3_AK=your-access-key
export DORIS_FS_TEST_S3_SK=your-secret-key

# Run S3-related environment tests
cd fe
mvn test -pl fe-filesystem/fe-filesystem-s3 \
  -Dtest.excludedGroups=none \
  -Dgroups=s3 \
  -Dcheckstyle.skip=true \
  -DfailIfNoTests=false \
  -Dmaven.build.cache.enabled=false \
  --also-make

# Run HDFS + Kerberos environment tests
mvn test -pl fe-filesystem/fe-filesystem-hdfs \
  -Dtest.excludedGroups=none \
  -Dgroups="hdfs | kerberos" \
  -Dcheckstyle.skip=true \
  -DfailIfNoTests=false \
  -Dmaven.build.cache.enabled=false \
  --also-make
```

## Environment Variables Reference

| Tag | Environment Variable | Description |
|-----|---------------------|-------------|
| `s3` | `DORIS_FS_TEST_S3_ENDPOINT` | S3-compatible storage endpoint |
| | `DORIS_FS_TEST_S3_REGION` | Region |
| | `DORIS_FS_TEST_S3_BUCKET` | Test bucket name |
| | `DORIS_FS_TEST_S3_AK` | Access Key |
| | `DORIS_FS_TEST_S3_SK` | Secret Key |
| `azure` | `DORIS_FS_TEST_AZURE_ACCOUNT` | Azure Storage account name |
| | `DORIS_FS_TEST_AZURE_KEY` | Account key |
| | `DORIS_FS_TEST_AZURE_CONTAINER` | Test container name |
| `cos` | `DORIS_FS_TEST_COS_ENDPOINT` | COS endpoint |
| | `DORIS_FS_TEST_COS_REGION` | Region (e.g., ap-guangzhou) |
| | `DORIS_FS_TEST_COS_BUCKET` | Test bucket name |
| | `DORIS_FS_TEST_COS_AK` | SecretId |
| | `DORIS_FS_TEST_COS_SK` | SecretKey |
| `oss` | `DORIS_FS_TEST_OSS_ENDPOINT` | OSS endpoint |
| | `DORIS_FS_TEST_OSS_REGION` | Region |
| | `DORIS_FS_TEST_OSS_BUCKET` | Test bucket name |
| | `DORIS_FS_TEST_OSS_AK` | Access Key |
| | `DORIS_FS_TEST_OSS_SK` | Secret Key |
| `obs` | `DORIS_FS_TEST_OBS_ENDPOINT` | OBS endpoint |
| | `DORIS_FS_TEST_OBS_REGION` | Region |
| | `DORIS_FS_TEST_OBS_BUCKET` | Test bucket name |
| | `DORIS_FS_TEST_OBS_AK` | Access Key |
| | `DORIS_FS_TEST_OBS_SK` | Secret Key |
| `hdfs` | `DORIS_FS_TEST_HDFS_HOST` | NameNode address |
| | `DORIS_FS_TEST_HDFS_PORT` | NameNode port |
| `kerberos` | `DORIS_FS_TEST_KDC_PRINCIPAL` | Kerberos principal |
| | `DORIS_FS_TEST_KDC_KEYTAB` | Keytab file path |
| | `DORIS_FS_TEST_HDFS_HOST` | Kerberos-enabled HDFS address |
| | `DORIS_FS_TEST_HDFS_PORT` | NameNode port (optional, defaults to 8020) |
| `broker` | `DORIS_FS_TEST_BROKER_HOST` | Broker process address |
| | `DORIS_FS_TEST_BROKER_PORT` | Broker process port |

## Test Case Overview

### T-E1: S3ObjStorage Environment Tests (6 tests, tag: `s3`)
- `putAndHeadObject` — Upload a small file → verify size and etag via headObject
- `listObjects` — Upload multiple files → verify returned count via listObjects
- `copyAndDeleteObject` — Upload → copy → verify → delete → verify non-existence
- `multipartUpload_completeSucceeds` — initiate → uploadPart × 2 → complete → verify via headObject
- `abortMultipartUpload_leavesNoObject` — initiate → abort → object does not exist
- `getPresignedUrl_returnsValidUrlAndUploadWorks` — Generate presigned URL → PUT upload → verify object exists

### T-E2: S3FileSystem Environment Tests (8 tests, tag: `s3`)
- `exists` — Existing / non-existing objects
- `deleteRemovesObject` — Upload → delete → exists returns false
- `renameMovesObject` — Upload → rename → old does not exist / new exists
- `listReturnsCorrectEntries` — Upload multiple → verify via list
- `inputOutputRoundTrip` — Write → read → content matches (including UTF-8 + emoji)
- `inputFileLength` — Upload with known size → verify length()
- `getPresignedUrl_returnsValidUrlAndUploadWorks` — Generate presigned URL → PUT upload → verify object exists

### T-E3: Azure Environment Tests (8 tests, tag: `azure`)
Same test cases as T-E2, using the `wasbs://` scheme.

### T-E3b/c/d: COS / OSS / OBS Environment Tests (8 tests each, tags: `cos` / `oss` / `obs`)
Same test cases as T-E2, using the Tencent Cloud / Alibaba Cloud / Huawei Cloud SDKs
respectively.

### T-E4: DFSFileSystem Environment Tests (8 tests, tag: `hdfs`)
- `mkdirsAndExists` — Create multi-level directories → exists returns true
- `deleteRecursive` — Create a directory with files → recursive delete
- `renameFile` / `renameDirectory` — File / directory rename
- `listFiles` / `listDirectories` — List files / subdirectories
- `inputOutputRoundTrip` — Write → read → content matches
- `inputFileLength` — Verify file size

### T-E5: Kerberos Environment Tests (4 tests, tag: `kerberos`)
- `loginSucceeds` — Log in with a real principal / keytab
- `doAsExecutesAction` — Verify return value of proxied execution
- `doAsPropagatesIOException` — IOException is correctly propagated
- `hdfsOperationWithKerberos` — HDFS exists works correctly under Kerberos

### T-E6: Broker Environment Tests (4 tests, tag: `broker`)
- `existsReturnsFalseForMissing` — Non-existing path → false
- `writeAndRead` — Write via outputFile → read via inputFile → content matches
- `deleteRemovesFile` — Write → delete → exists returns false
- `listReturnsFiles` — Write multiple → verify via list

## Notes

1. **Each test creates temporary data with a unique UUID prefix in the target storage**.
   `@AfterAll` attempts cleanup. If a test is interrupted abnormally, manual cleanup may be
   required.

2. **S3 multipart upload tests** upload ~5 MB of data. Ensure the test bucket has sufficient
   space and permissions.

3. **Environment tests are not run by default** (the module POM sets
   `<excludedGroups>${test.excludedGroups}</excludedGroups>` with a default value of
   `environment`). They only run when explicitly enabled with
   `-Dtest.excludedGroups=none -Dgroups=<tag>`.

4. **Never commit credentials to the code repository**. Use environment variables or CI/CD
   secret management instead.
