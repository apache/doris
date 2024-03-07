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

suite("test_iceberg_table_stats", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
            String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String catalog_name = "test_iceberg_rest_catalog"
            String db_name = "format_v2"

            sql """drop catalog if exists ${catalog_name}"""
            sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                    'type'='iceberg',
                    'iceberg.catalog.type'='rest',
                    'uri' = 'http://${externalEnvIp}:${rest_port}',
                    "s3.access_key" = "admin",
                    "s3.secret_key" = "password",
                    "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                    "s3.region" = "us-east-1"
                );"""

            def assert_stats = { table_name, cnt -> 
                def retry = 0
                def act = ""
                while (retry < 10) {
                    def result = sql """ show table stats ${table_name} """
                    act = result[0][2]
                    if (act != "0") {
                        break;
                    }
                    Thread.sleep(2000)
                    retry++
                }
                assertEquals(act, cnt)
            }

            sql """ switch ${catalog_name} """
            sql """ use format_v2 """
            assert_stats("sample_cow_orc", "1000")
            assert_stats("sample_cow_parquet", "1000")
            assert_stats("sample_mor_orc", "1000")
            assert_stats("sample_mor_parquet", "1000")

        } finally {
        }
    }
}

