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

suite("test_iceberg_write_partitions", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    def format_compressions = ["parquet_snappy", "orc_zlib"]

    def q01 = { String format_compression, String catalog_name, String hive_catalog_name ->
        def parts = format_compression.split("_")
        def format = parts[0]
        def compression = parts[1]
        sql """ DROP TABLE IF EXISTS `iceberg_all_partition_types1_${format_compression}` """
        sql """
        CREATE TABLE `iceberg_all_partition_types1_${format_compression}`(
          `boolean_col` boolean,
          `id` int,
          `int_col` int,
          `bigint_col` bigint,
          `float_col` float,
          `double_col` double)
          ENGINE=iceberg
          PARTITION BY LIST (boolean_col, int_col, bigint_col, float_col, double_col) ()
          properties (
            "compression-codec" = ${compression},
            "write-format"=${format}
          )
        """

        sql """
        INSERT INTO iceberg_all_partition_types1_${format_compression}
        SELECT boolean_col, id, int_col, bigint_col, float_col, double_col FROM ${hive_catalog_name}.write_test.all_partition_types1_parquet_snappy_src where id = 1;
        """
        order_qt_q01 """ select * from iceberg_all_partition_types1_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types1_${format_compression}
        SELECT boolean_col, id, int_col, bigint_col, float_col, double_col FROM ${hive_catalog_name}.write_test.all_partition_types1_parquet_snappy_src where id = 2;
        """
        order_qt_q02 """ select * from iceberg_all_partition_types1_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types1_${format_compression}
        SELECT boolean_col, id, int_col, bigint_col, float_col, double_col FROM ${hive_catalog_name}.write_test.all_partition_types1_parquet_snappy_src where id = 3;
        """
        order_qt_q03 """ select * from iceberg_all_partition_types1_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types1_${format_compression}
        SELECT boolean_col, id, int_col, null, float_col, null FROM ${hive_catalog_name}.write_test.all_partition_types1_parquet_snappy_src where id = 3;
        """
        order_qt_q04 """ select * from iceberg_all_partition_types1_${format_compression};
        """

        sql """ DROP TABLE iceberg_all_partition_types1_${format_compression}; """
    }

    def q02 = { String format_compression, String catalog_name, String hive_catalog_name ->
        def parts = format_compression.split("_")
        def format = parts[0]
        def compression = parts[1]
        sql """ DROP TABLE IF EXISTS `iceberg_all_partition_types2_${format_compression}` """
        sql """
        CREATE TABLE `iceberg_all_partition_types2_${format_compression}`(
            `decimal_col` decimal(18,6),
            `id` int,
            `string_col` string,
            `date_col` date)
          ENGINE=iceberg
          PARTITION BY LIST (decimal_col, string_col, date_col) ()
          properties (
            "compression-codec" = ${compression},
            "write-format"=${format}
          )
        """

        sql """
        INSERT INTO iceberg_all_partition_types2_${format_compression}
        SELECT decimal_col, id, string_col, date_col FROM ${hive_catalog_name}.write_test.all_partition_types2_parquet_snappy_src where id = 1;
        """
        order_qt_q01 """ select * from iceberg_all_partition_types2_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types2_${format_compression}
        SELECT decimal_col, id, string_col, date_col FROM ${hive_catalog_name}.write_test.all_partition_types2_parquet_snappy_src where id = 2;
        """
        order_qt_q02 """ select * from iceberg_all_partition_types2_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types2_${format_compression}
        SELECT decimal_col, id, string_col, date_col FROM ${hive_catalog_name}.write_test.all_partition_types2_parquet_snappy_src where id = 3;
        """
        order_qt_q03 """ select * from iceberg_all_partition_types2_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types2_${format_compression}
        SELECT decimal_col, id, null, date_col FROM ${hive_catalog_name}.write_test.all_partition_types2_parquet_snappy_src where id = 3;
        """
        order_qt_q04 """ select * from iceberg_all_partition_types2_${format_compression};
        """

        sql """ DROP TABLE iceberg_all_partition_types2_${format_compression}; """
    }

    def test_columns_out_of_order = {  String format_compression, String catalog_name ->
        def parts = format_compression.split("_")
        def format = parts[0]
        def compression = parts[1]
        sql """ drop table if exists columns_out_of_order_source_tbl_${format_compression} """
        sql """
            CREATE TABLE columns_out_of_order_source_tbl_${format_compression} (
                `col3` bigint,
                `col6` int,
                `col1` bigint,
                `col4` int,
                `col2` bigint,
                `col5` int
                ) ENGINE = iceberg
                properties (
                    "compression-codec" = ${compression},
                    "write-format"=${format}
                )
        """;
        sql """ drop table if exists columns_out_of_order_target_tbl_${format_compression} """
        sql """
            CREATE TABLE columns_out_of_order_target_tbl_${format_compression} (
                `col1` bigint,
                `col2` bigint,
                `col3` bigint,
                `col4` int,
                `col5` int,
                `col6` int
                ) ENGINE = iceberg
                PARTITION BY LIST (
                      col4, col5, col6
                )()
                properties (
                    "compression-codec" = ${compression},
                    "write-format"=${format}
                )
        """;

        sql """
            INSERT INTO columns_out_of_order_source_tbl_${format_compression} (
              col1, col2, col3, col4, col5, col6
            ) VALUES (1, 2, 3, 4, 5, 6);
            """
        order_qt_columns_out_of_order01 """ SELECT * FROM columns_out_of_order_source_tbl_${format_compression} """

        sql """
            INSERT INTO columns_out_of_order_target_tbl_${format_compression} (
              col1, col2, col3, col4, col5, col6
            ) VALUES (1, 2, 3, 4, 5, 6);
            """

        order_qt_columns_out_of_order02 """ SELECT * FROM columns_out_of_order_target_tbl_${format_compression} """

        sql """ drop table columns_out_of_order_source_tbl_${format_compression} """
        sql """ drop table columns_out_of_order_target_tbl_${format_compression} """
        sql """ drop database if exists `test_columns_out_of_order` """;
    }


    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String iceberg_catalog_name = "test_iceberg_write_partitions_iceberg_${hivePrefix}"
            String hive_catalog_name = "test_iceberg_write_partitions_hive_${hivePrefix}"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${iceberg_catalog_name}"""
            sql """create catalog if not exists ${iceberg_catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
            sql """drop catalog if exists ${hive_catalog_name}"""
            sql """create catalog if not exists ${hive_catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""

            sql """use `${iceberg_catalog_name}`.`write_test`"""

            sql """set enable_fallback_to_original_planner=false;"""

            for (String format_compression in format_compressions) {
                logger.info("Process format_compression " + format_compression)
                q01(format_compression, iceberg_catalog_name, hive_catalog_name)
                q02(format_compression, iceberg_catalog_name, hive_catalog_name)
                test_columns_out_of_order(format_compression, iceberg_catalog_name)
            }
            sql """drop catalog if exists ${iceberg_catalog_name}"""
            sql """drop catalog if exists ${hive_catalog_name}"""
        } finally {
        }
    }
}
