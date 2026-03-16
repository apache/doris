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

suite("test_iceberg_deletion_vector", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_deletion_vector"

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


    sql """switch ${catalog_name};"""
    sql """ use format_v3;"""


    qt_q1 """ SELECT * FROM dv_test ORDER BY id; """
    qt_q2 """ SELECT * FROM dv_test_v2 ORDER BY id; """

    qt_q4 """ SELECT * FROM dv_test where id = 2 ORDER BY id ; """
    qt_q5 """ SELECT * FROM dv_test_v2 where id = 2 ORDER BY id  ; """

    qt_q6 """ SELECT * FROM dv_test  where id %2 =1 ORDER BY id; """
    qt_q7 """ SELECT * FROM dv_test_v2  where id %2 =1 ORDER BY id; """

    qt_q8 """ SELECT * FROM dv_test where data < 'f' ORDER BY id; """
    qt_q9 """ SELECT * FROM dv_test_v2 where data < 'f' ORDER BY id; """


    qt_q10 """ SELECT count(*) FROM dv_test ; """
    qt_q11 """ SELECT count(*) FROM dv_test_v2 ; """


    qt_q12 """ SELECT count(id) FROM dv_test ; """
    qt_q13 """ SELECT count(id) FROM dv_test_v2 ; """
    
    qt_q14 """ SELECT count(batch) FROM dv_test ; """
    qt_q15 """ SELECT count(batch) FROM dv_test_v2 ; """


    qt_q16 """ SELECT count(*) FROM dv_test_1w ; """
    qt_q17 """ SELECT count(id) FROM dv_test_1w ; """
    qt_q18 """ SELECT count(grp) FROM dv_test_1w where id = 1; """
    qt_q19 """ SELECT count(value) FROM dv_test_1w where id%2 = 1; """
    qt_q20 """ SELECT count(id) FROM dv_test_1w where id%3 = 1; """
    qt_q21 """ SELECT count(ts) FROM dv_test_1w where id%3 != 1; """





}
