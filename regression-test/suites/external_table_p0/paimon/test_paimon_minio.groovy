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

suite("test_paimon_minio", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
            String catalog_name = "test_paimon_minio"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String table_name = "ts_scale_orc"

            sql """drop catalog if exists ${catalog_name}"""

            sql """
                CREATE CATALOG ${catalog_name} PROPERTIES (
                        'type' = 'paimon',
                        'warehouse' = 's3://warehouse/wh',
                        's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                        's3.access_key' = 'admin',
                        's3.secret_key' = 'password',
                        's3.path.style.access' = 'true'
                );
            """
            sql """switch `${catalog_name}`"""
            sql """show databases; """
            sql """use `${catalog_name}`.`flink_paimon`"""
            order_qt_no_region1 """select ts1,ts19 from ${table_name} """
            order_qt_no_region2 """select * from ${table_name} """
            sql """drop catalog if exists ${catalog_name}"""

            sql """drop catalog if exists ${catalog_name}_with_region"""
            sql """
                CREATE CATALOG ${catalog_name}_with_region PROPERTIES (
                        'type' = 'paimon',
                        'warehouse' = 's3://warehouse/wh',
                        's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                        's3.access_key' = 'admin',
                        's3.secret_key' = 'password',
                        's3.region' = 'us-west-2',
                        's3.path.style.access' = 'true'
                );
            """
            sql """switch `${catalog_name}_with_region`"""
            sql """show databases; """
            sql """use `${catalog_name}_with_region`.`flink_paimon`"""
            order_qt_region1 """select ts1,ts19 from ${table_name} """
            order_qt_region2 """select * from ${table_name} """
            sql """drop catalog if exists ${catalog_name}_with_region"""
        }
}


