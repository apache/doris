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

suite("test_iceberg_mtmv", "p0,external,iceberg,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    logger.info("enabled: " + enabled)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    logger.info("externalEnvIp: " + externalEnvIp)
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    logger.info("rest_port: " + rest_port)
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    logger.info("minio_port: " + minio_port)

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "iceberg_mtmv_catalog";
        String mvName = "test_iceberg_mtmv"
        String dbName = "regression_test_mtmv_p0"
        String icebergDb = "format_v2"
        String icebergTable = "sample_mor_parquet"
        sql """drop catalog if exists ${catalog_name} """

        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${rest_port}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1"
        );"""

        order_qt_catalog """select count(*) from ${catalog_name}.${icebergDb}.${icebergTable}"""
        sql """drop materialized view if exists ${mvName};"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${catalog_name}.${icebergDb}.${icebergTable};
            """

        sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
        def jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv "SELECT count(*) FROM ${mvName}"

        sql """drop materialized view if exists ${mvName};"""
        sql """ drop catalog if exists ${catalog_name} """
    }
}

