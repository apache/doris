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

suite("test_external_catalog_iceberg_partition", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_external_catalog_iceberg_partition"

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
        sql """ use multi_catalog; """
        // test parquet format
        def q01_parquet = {
            qt_q01 """ select * from parquet_partitioned_one_column order by t_float """
            qt_q02 """ select * from parquet_partitioned_one_column where t_int is null order by t_float """
            qt_q03 """ select * from parquet_partitioned_one_column where t_int is not null order by t_float """
            qt_q04 """ select * from parquet_partitioned_columns order by t_float """
            qt_q05 """ select * from parquet_partitioned_columns where t_int is null order by t_float """
            qt_q06 """ select * from parquet_partitioned_columns where t_int is not null order by t_float """
            qt_q07 """ select * from parquet_partitioned_truncate_and_fields order by t_float """
            qt_q08 """ select * from parquet_partitioned_truncate_and_fields where t_int is null order by t_float """
            qt_q09 """ select * from parquet_partitioned_truncate_and_fields where t_int is not null order by t_float """
        }
        // test orc format
        def q01_orc = {
            qt_q01 """ select * from orc_partitioned_one_column order by t_float """
            qt_q02 """ select * from orc_partitioned_one_column where t_int is null order by t_float """
            qt_q03 """ select * from orc_partitioned_one_column where t_int is not null order by t_float """
            qt_q04 """ select * from orc_partitioned_columns order by t_float """
            qt_q05 """ select * from orc_partitioned_columns where t_int is null order by t_float """
            qt_q06 """ select * from orc_partitioned_columns where t_int is not null order by t_float """
            qt_q07 """ select * from orc_partitioned_truncate_and_fields order by t_float """
            qt_q08 """ select * from orc_partitioned_truncate_and_fields where t_int is null order by t_float """
            qt_q09 """ select * from orc_partitioned_truncate_and_fields where t_int is not null order by t_float """
        }

        // test date for partition and predict
        def q01_date = {

            qt_q01 """ select * from user_case_date_without_partition where d = '2020-01-02' """
            qt_q02 """ select * from user_case_date_without_partition where d > '2020-01-01' """
            qt_q03 """ select * from user_case_date_without_partition where d < '2020-01-03' """
            qt_q04 """ select * from user_case_date_without_partition where ts < '2020-01-03' """
            qt_q05 """ select * from user_case_date_without_partition where ts > '2020-01-01' """

            qt_q06 """ select * from user_case_date_with_date_partition where d = '2020-01-02' """
            qt_q07 """ select * from user_case_date_with_date_partition where d < '2020-01-03' """
            qt_q08 """ select * from user_case_date_with_date_partition where d > '2020-01-01' """

            qt_q09 """ select * from user_case_date_with_days_date_partition where d = '2020-01-02' """
            qt_q10 """ select * from user_case_date_with_days_date_partition where d < '2020-01-03' """
            qt_q11 """ select * from user_case_date_with_days_date_partition where d > '2020-01-01' """

        }

        q01_parquet()
        q01_orc()
        q01_date()
}

