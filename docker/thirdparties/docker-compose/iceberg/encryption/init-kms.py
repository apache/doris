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

import os
import pathlib

import boto3
from botocore.exceptions import ClientError

client = boto3.client("kms", endpoint_url="http://encryption-kms:5000",
                      region_name="us-east-1", aws_access_key_id="admin",
                      aws_secret_access_key="password")
master = client.create_key(Description="Disposable Iceberg metadata fixture")["KeyMetadata"]["KeyId"]
state = pathlib.Path("/state")
state.mkdir(exist_ok=True)
temporary = state / "kms.properties.tmp"
temporary.write_text("key-id=" + master + "\n", encoding="utf-8")
os.replace(temporary, state / "kms.properties")
print("Disposable test KMS initialized")

# This bucket is separate from the existing Spark/REST fixture warehouse.
s3 = boto3.client("s3", endpoint_url="http://minio:9000", region_name="us-east-1",
                  aws_access_key_id="admin", aws_secret_access_key="password")
try:
    s3.head_bucket(Bucket="iceberg-encryption")
except ClientError as error:
    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
        raise
    s3.create_bucket(Bucket="iceberg-encryption")
