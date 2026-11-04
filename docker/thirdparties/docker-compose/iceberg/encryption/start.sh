#!/bin/bash
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

classpath="/fixtures/jars/iceberg-spark-runtime-4.0_2.13-1.12.0.jar:/fixtures/jars/iceberg-aws-bundle-1.12.0.jar:/opt/spark/jars/*"
mkdir -p /tmp/encryption-classes
javac -cp "${classpath}" -d /tmp/encryption-classes /fixtures/IcebergEncryptionFixture.java
exec java -XX:ActiveProcessorCount=4 -Xmx1g -cp "/tmp/encryption-classes:${classpath}" \
    IcebergEncryptionFixture thrift://encryption-hms:9083 http://encryption-kms:5000 \
    http://minio:9000 /state/kms.properties
