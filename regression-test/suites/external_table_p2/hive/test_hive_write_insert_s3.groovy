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

suite("test_hive_write_insert_s3", "p2,external,hive,external_remote,external_remote_hive") {
    def format_compressions = ["parquet_snappy"]
    def s3BucketName = getS3BucketName()

    def q01 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ truncate table all_types_${format_compression}_s3; """)
        hive_remote """ truncate table all_types_${format_compression}_s3; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_${format_compression}_s3
        SELECT * FROM all_types_parquet_snappy_src;
        """
        order_qt_q01 """ select * from all_types_${format_compression}_s3;
        """

        sql """
        INSERT INTO all_types_${format_compression}_s3
        SELECT boolean_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, decimal_col1, decimal_col2,
         decimal_col3, decimal_col4, string_col, binary_col, date_col, timestamp_col1, timestamp_col2, timestamp_col3, char_col1,
          char_col2, char_col3, varchar_col1, varchar_col2, varchar_col3, t_map_string, t_map_varchar, t_map_char, t_map_int,
           t_map_bigint, t_map_float, t_map_double, t_map_boolean, t_map_decimal_precision_2, t_map_decimal_precision_4,
            t_map_decimal_precision_8, t_map_decimal_precision_17, t_map_decimal_precision_18, t_map_decimal_precision_38,
             t_array_string, t_array_int, t_array_bigint, t_array_float, t_array_double, t_array_boolean, t_array_varchar,
              t_array_char, t_array_decimal_precision_2, t_array_decimal_precision_4, t_array_decimal_precision_8,
               t_array_decimal_precision_17, t_array_decimal_precision_18, t_array_decimal_precision_38, t_struct_bigint, t_complex,
                t_struct_nested, t_struct_null, t_struct_non_nulls_after_nulls, t_nested_struct_non_nulls_after_nulls,
                 t_map_null_value, t_array_string_starting_with_nulls, t_array_string_with_nulls_in_between,
                  t_array_string_ending_with_nulls, t_array_string_all_nulls, dt FROM all_types_parquet_snappy_src;
        """
        order_qt_q02 """ select * from all_types_${format_compression}_s3;
        """

        sql """
        INSERT INTO all_types_${format_compression}_s3(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls FROM all_types_parquet_snappy_src;
        """
        order_qt_q03 """
        select * from all_types_${format_compression}_s3;
        """

        sql """
        INSERT OVERWRITE TABLE all_types_${format_compression}_s3(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls FROM all_types_parquet_snappy_src;
        """
        order_qt_q04 """
        select * from all_types_${format_compression}_s3;
        """

        logger.info("hive sql: " + """ truncate table all_types_${format_compression}_s3; """)
        hive_remote """ truncate table all_types_${format_compression}_s3; """
        sql """refresh catalog ${catalog_name};"""
        order_qt_q05 """
        select * from all_types_${format_compression}_s3;
        """
    }

    def q02 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_s3_${catalog_name}_q02; """)
        hive_remote """ DROP TABLE IF EXISTS all_types_par_${format_compression}_s3_${catalog_name}_q02; """
        logger.info("hive sql: " + """ CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_s3_${catalog_name}_q02 like all_types_par_${format_compression}_s3; """)
        hive_remote """ CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_s3_${catalog_name}_q02 like all_types_par_${format_compression}_s3; """
        logger.info("hive sql: " + """ ALTER TABLE all_types_par_${format_compression}_s3_${catalog_name}_q02 SET LOCATION 'cosn://${s3BucketName}/regression/write/data/all_types_par_${format_compression}_s3_${catalog_name}_q02'; """)
        hive_remote """ ALTER TABLE all_types_par_${format_compression}_s3_${catalog_name}_q02 SET LOCATION 'cosn://${s3BucketName}/regression/write/data/all_types_par_${format_compression}_s3_${catalog_name}_q02'; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_par_${format_compression}_s3_${catalog_name}_q02
        SELECT * FROM all_types_par_parquet_snappy_src;
        """
        order_qt_q01 """ select * from all_types_par_${format_compression}_s3_${catalog_name}_q02;
        """

        sql """
        INSERT INTO all_types_par_${format_compression}_s3_${catalog_name}_q02
        SELECT boolean_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, decimal_col1, decimal_col2,
         decimal_col3, decimal_col4, string_col, binary_col, date_col, timestamp_col1, timestamp_col2, timestamp_col3, char_col1,
          char_col2, char_col3, varchar_col1, varchar_col2, varchar_col3, t_map_string, t_map_varchar, t_map_char, t_map_int,
           t_map_bigint, t_map_float, t_map_double, t_map_boolean, t_map_decimal_precision_2, t_map_decimal_precision_4,
            t_map_decimal_precision_8, t_map_decimal_precision_17, t_map_decimal_precision_18, t_map_decimal_precision_38,
             t_array_string, t_array_int, t_array_bigint, t_array_float, t_array_double, t_array_boolean, t_array_varchar,
              t_array_char, t_array_decimal_precision_2, t_array_decimal_precision_4, t_array_decimal_precision_8,
               t_array_decimal_precision_17, t_array_decimal_precision_18, t_array_decimal_precision_38, t_struct_bigint, t_complex,
                t_struct_nested, t_struct_null, t_struct_non_nulls_after_nulls, t_nested_struct_non_nulls_after_nulls,
                 t_map_null_value, t_array_string_starting_with_nulls, t_array_string_with_nulls_in_between,
                  t_array_string_ending_with_nulls, t_array_string_all_nulls, dt FROM all_types_parquet_snappy_src;
        """
        order_qt_q02 """ select * from all_types_par_${format_compression}_s3_${catalog_name}_q02;
        """

        sql """
        INSERT INTO all_types_par_${format_compression}_s3_${catalog_name}_q02(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt FROM all_types_parquet_snappy_src;
        """
        order_qt_q03 """ select * from all_types_par_${format_compression}_s3_${catalog_name}_q02;
        """

        sql """
        INSERT OVERWRITE TABLE all_types_par_${format_compression}_s3_${catalog_name}_q02(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt FROM all_types_parquet_snappy_src;
        """
        order_qt_q04 """
        select * from all_types_par_${format_compression}_s3_${catalog_name}_q02;
        """

        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_s3_${catalog_name}_q02; """)
        hive_remote """ DROP TABLE IF EXISTS all_types_par_${format_compression}_s3_${catalog_name}_q02; """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String catalog_name = "test_hive_write_insert_s3"

            String hms_host = context.config.otherConfigs.get("extHiveHmsHost")
            String Hms_port = context.config.otherConfigs.get("extHiveHmsPort")
            String hdfs_host = context.config.otherConfigs.get("extHiveHmsHost")
            String hdfs_port = context.config.otherConfigs.get("extHdfsPort")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String ak = context.config.otherConfigs.get("extAk")
            String sk = context.config.otherConfigs.get("extSk")
            String endpoint = context.config.otherConfigs.get("extS3Endpoint")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${hms_host}:${Hms_port}',
                'fs.defaultFS' = 'hdfs://${hdfs_host}:${hdfs_port}',
                'hadoop.username' = 'hadoop',
                's3.endpoint' = '${endpoint}',
                's3.access_key' = '${ak}',
                's3.secret_key' = '${sk}'
            );"""
            sql """use `${catalog_name}`.`write_test`"""
            logger.info("hive sql: " + """ use `write_test` """)
            hive_remote """use `write_test`"""

            sql """set enable_fallback_to_original_planner=false;"""

            for (String format_compression in format_compressions) {
                logger.info("Process format_compression" + format_compression)
                q01(format_compression, catalog_name)
                q02(format_compression, catalog_name)
            }

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
