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

suite("test_iceberg_write_stats2", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    for (String hivePrefix : ["hive2"]) {
        setHivePrefix(hivePrefix)
        String catalog_name = "test_iceberg_write_stats"
        String db_name = "test_stats"
        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        sql """drop catalog if exists ${catalog_name}"""
        sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${rest_port}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1",
            "enable.mapping.varbinary"="true"
        );"""

        sql """switch ${catalog_name}"""
        sql """create database if not exists ${db_name}"""
        sql """use ${db_name}"""

        sql """ DROP TABLE IF EXISTS `iceberg_all_types_parquet`; """

        sql """
        CREATE TABLE `iceberg_all_types_parquet`(
            `boolean_col` boolean,
            `int_col` int,
            `bigint_col` bigint,
            `float_col` float,
            `double_col` double,
            `decimal_col1` decimal(9,0),
            `decimal_col2` decimal(8,4),
            `decimal_col3` decimal(18,6),
            `decimal_col4` decimal(38,12),
            `string_col` string,
            `date_col` date,
            `datetime_col1` datetime) 
            ENGINE=iceberg
            properties (
            "write-format"="parquet"
            );
        """

        sql """ 
        INSERT INTO iceberg_all_types_parquet VALUES
        (true, 11, 111, 1.1, 1.1, 1111.0, 1234.5678, 1234.56789, 123456789012345678.123456789012, 'aaa', '2023-01-01', '2023-01-01 12:34:56'),
        (false, 22, 222, 2.2, 2.2, 2222.0, 8765.4321, 8765.43210, 987654321098765432.98765432109876, 'bbb', '2023-06-15', '2023-06-15 23:45:01');
        """
        
        qt_sql_0 """ select * from iceberg_all_types_parquet order by 1; """;
        qt_sql_1 """ select content,file_format,record_count,value_counts,null_value_counts,lower_bounds,upper_bounds from iceberg_all_types_parquet\$files order by 1; """;
        qt_sql_2 """ select readable_metrics from iceberg_all_types_parquet\$files order by 1; """;


        sql """ DROP TABLE IF EXISTS `iceberg_all_types_orc`; """

        sql """
        CREATE TABLE `iceberg_all_types_orc`(
            `boolean_col` boolean,
            `int_col` int,
            `bigint_col` bigint,
            `float_col` float,
            `double_col` double,
            `decimal_col1` decimal(9,0),
            `decimal_col2` decimal(8,4),
            `decimal_col3` decimal(18,6),
            `decimal_col4` decimal(38,12),
            `string_col` string,
            `date_col` date,
            `datetime_col1` datetime) 
            ENGINE=iceberg
            properties (
            "write-format"="orc"
            );
        """

        sql """ 
        INSERT INTO iceberg_all_types_orc VALUES
        (true, 11, 111, 1.1, 1.1, 1111.0, 1234.5678, 1234.56789, 123456789012345678.123456789012, 'aaa', '2023-01-01', '2023-01-01 12:34:56'),
        (false, 22, 222, 2.2, 2.2, 2222.0, 8765.4321, 8765.43210, 987654321098765432.98765432109876, 'bbb', '2023-06-15', '2023-06-15 23:45:01');
        """
        
        qt_sql_3 """ select * from iceberg_all_types_orc order by 1; """;
        qt_sql_4 """ select content,file_format,record_count,value_counts,null_value_counts,lower_bounds,upper_bounds from iceberg_all_types_orc\$files order by 1; """;
        qt_sql_5 """ select readable_metrics from iceberg_all_types_orc\$files order by 1; """;
        qt_sql_6 """ select lower_bounds from iceberg_all_types_parquet\$files order by 1; """;
        qt_sql_7 """ select lower_bounds from iceberg_all_types_orc\$files order by 1; """;
        qt_sql_8 """ select upper_bounds from iceberg_all_types_parquet\$files order by 1; """;
        qt_sql_9 """ select upper_bounds from iceberg_all_types_orc\$files order by 1; """;
    }
}
