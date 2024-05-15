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

    def q01 = { String format_compression, String catalog_name ->
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
        SELECT id, boolean_col, int_col, bigint_col, float_col, double_col FROM all_partition_types1_parquet_snappy_src where id = 1;
        """
        order_qt_q01 """ select * from iceberg_all_partition_types1_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types1_${format_compression}
        SELECT id, boolean_col, int_col, bigint_col, float_col, double_col FROM all_partition_types1_parquet_snappy_src where id = 2;
        """
        order_qt_q02 """ select * from iceberg_all_partition_types1_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types1_${format_compression}
        SELECT id, boolean_col, int_col, bigint_col, float_col, double_col FROM all_partition_types1_parquet_snappy_src where id = 3;
        """
        order_qt_q03 """ select * from iceberg_all_partition_types1_${format_compression};
        """

        sql """ DROP TABLE iceberg_all_partition_types1_${format_compression}; """
    }

    def q02 = { String format_compression, String catalog_name ->
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
        SELECT decimal_col, id, string_col, date_col FROM all_partition_types2_parquet_snappy_src where id = 1;
        """
        order_qt_q01 """ select * from iceberg_all_partition_types2_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types2_${format_compression}
        SELECT decimal_col, id, string_col, date_col FROM all_partition_types2_parquet_snappy_src where id = 2;
        """
        order_qt_q02 """ select * from iceberg_all_partition_types2_${format_compression};
        """

        sql """
        INSERT INTO iceberg_all_partition_types2_${format_compression}
        SELECT decimal_col, id, string_col, date_col FROM all_partition_types2_parquet_snappy_src where id = 3;
        """
        order_qt_q03 """ select * from iceberg_all_partition_types2_${format_compression};
        """

        sql """ DROP TABLE iceberg_all_partition_types2_${format_compression}; """
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
            String catalog_name = "test_${hivePrefix}_write_partitions"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );"""
            sql """use `${catalog_name}`.`write_test`"""

            sql """set enable_fallback_to_original_planner=false;"""

            for (String format_compression in format_compressions) {
                logger.info("Process format_compression " + format_compression)
                q01(format_compression, catalog_name)
                q02(format_compression, catalog_name)
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
