// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

testDirectories = "vault_p0"
max_failure_num = 10

jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
targetJdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"

// for vault case, consistent with the configuration in the create_warehouse_vault method in the doris-utils.sh file.
instanceId="cloud_instance_0"
multiClusterInstanceId="cloud_instance_0"

hdfsFs = "hdfs://127.0.0.1:8020"
hdfsUser = "root"
hdfsPasswd = ""

extHiveHmsHost = "127.0.0.1"
extHiveHmsPort = 7004
extHdfsPort = 8020
extHiveServerPort= 7001
extHiveHmsUser = "root"

// for case test_minio_storage_vault.groovy
extMinioHost = "127.0.0.1"
extMinioPort = 19000
extMinioDomain = "myminio.com"
extMinioAk = "minioadmin"
extMinioSk = "minioadmin"
extMinioRegion = "us-east-1"
extMinioBucket = "test-bucket"

s3Endpoint = "oss-cn-hongkong-internal.aliyuncs.com"
