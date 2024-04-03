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

suite("test_external_catalog_iceberg_partition", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_external_catalog_iceberg_partition"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """switch ${catalog_name};"""
        // test parquet format
        def q01_parquet = {
            qt_q01 """ select * from iceberg_catalog.parquet_partitioned_one_column order by t_float """
            qt_q02 """ select * from iceberg_catalog.parquet_partitioned_one_column where t_int is null order by t_float """
            qt_q03 """ select * from iceberg_catalog.parquet_partitioned_one_column where t_int is not null order by t_float """
            qt_q04 """ select * from iceberg_catalog.parquet_partitioned_columns order by t_float """
            qt_q05 """ select * from iceberg_catalog.parquet_partitioned_columns where t_int is null order by t_float """
            qt_q06 """ select * from iceberg_catalog.parquet_partitioned_columns where t_int is not null order by t_float """
            qt_q07 """ select * from iceberg_catalog.parquet_partitioned_truncate_and_fields order by t_float """
            qt_q08 """ select * from iceberg_catalog.parquet_partitioned_truncate_and_fields where t_int is null order by t_float """
            qt_q09 """ select * from iceberg_catalog.parquet_partitioned_truncate_and_fields where t_int is not null order by t_float """
        }
        // test orc format
        def q01_orc = {
            qt_q01 """ select * from iceberg_catalog.orc_partitioned_one_column order by t_float """
            qt_q02 """ select * from iceberg_catalog.orc_partitioned_one_column where t_int is null order by t_float """
            qt_q03 """ select * from iceberg_catalog.orc_partitioned_one_column where t_int is not null order by t_float """
            qt_q04 """ select * from iceberg_catalog.orc_partitioned_columns order by t_float """
            qt_q05 """ select * from iceberg_catalog.orc_partitioned_columns where t_int is null order by t_float """
            qt_q06 """ select * from iceberg_catalog.orc_partitioned_columns where t_int is not null order by t_float """
            qt_q07 """ select * from iceberg_catalog.orc_partitioned_truncate_and_fields order by t_float """
            qt_q08 """ select * from iceberg_catalog.orc_partitioned_truncate_and_fields where t_int is null order by t_float """
            qt_q09 """ select * from iceberg_catalog.orc_partitioned_truncate_and_fields where t_int is not null order by t_float """
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

        sql """ use `iceberg_catalog`; """
        q01_parquet()
        q01_orc()
        q01_date()
    }
}

