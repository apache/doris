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

suite("test_iceberg_time_travel", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_time_travel"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

        sql """ use ${catalog_name}.test_db """

        // test for 'FOR TIME AS OF' and 'FOR VERSION AS OF'
        def q01 = {
            qt_q1 """ select count(*) from iceberg_position_gen_data """  // 5632
            qt_q2 """ select count(*) from iceberg_position_gen_data FOR TIME AS OF '2024-07-14 14:17:01' """// 120
            qt_q3 """ select * from iceberg_position_gen_data order by id limit 10""" 
            qt_q4 """ select * from iceberg_position_gen_data FOR TIME AS OF '2024-07-14 14:17:01' order by id limit 10""" 
            qt_q5 """ select count(*) from iceberg_position_gen_data FOR VERSION AS OF 3106988132043095748 """ // 240
            qt_q6 """ select * from iceberg_position_gen_data FOR VERSION AS OF 3106988132043095748 order by id limit 10""" 
        }
        
        q01()
}
