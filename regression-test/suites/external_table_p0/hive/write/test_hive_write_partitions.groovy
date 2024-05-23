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

suite("test_hive_write_partitions", "p0,external,hive,external_docker,external_docker_hive") {
    def format_compressions = ["parquet_snappy", "orc_zlib"]

    def q01 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q01; """)
        hive_docker """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q01; """
        logger.info("hive sql: " + """ CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_${catalog_name}_q01 like all_types_par_${format_compression}; """)
        hive_docker """ CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_${catalog_name}_q01 like all_types_par_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q01
        SELECT * FROM all_types_par_parquet_snappy_src;
        """
        order_qt_q01 """ select * from all_types_par_${format_compression}_${catalog_name}_q01;
        """

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q01
        SELECT * FROM all_types_par_parquet_snappy_src;
        """
        order_qt_q02 """ select * from all_types_par_${format_compression}_${catalog_name}_q01;
        """

        sql """
        INSERT OVERWRITE TABLE all_types_par_${format_compression}_${catalog_name}_q01
        SELECT * FROM all_types_par_parquet_snappy_src;
        """
        order_qt_q03 """ select * from all_types_par_${format_compression}_${catalog_name}_q01;
        """

        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q01; """)
        hive_docker """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q01; """
    }


    def q02 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q02; """)
        hive_docker """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q02; """
        logger.info("hive sql: " + """ CREATE TABLE IF NOT EXISTS all_partition_types1_${format_compression}_${catalog_name}_q02 like all_partition_types1_${format_compression}; """)
        hive_docker """ CREATE TABLE IF NOT EXISTS all_partition_types1_${format_compression}_${catalog_name}_q02 like all_partition_types1_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_partition_types1_${format_compression}_${catalog_name}_q02
        SELECT * FROM all_partition_types1_parquet_snappy_src where id = 1;
        """
        order_qt_q01 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q02;
        """

        sql """
        INSERT INTO all_partition_types1_${format_compression}_${catalog_name}_q02
        SELECT * FROM all_partition_types1_parquet_snappy_src where id = 2;
        """
        order_qt_q02 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q02;
        """

        sql """
        INSERT OVERWRITE TABLE all_partition_types1_${format_compression}_${catalog_name}_q02
        SELECT * FROM all_partition_types1_parquet_snappy_src;
        """
        order_qt_q03 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q02;
        """

        sql """
        INSERT INTO all_partition_types1_${format_compression}_${catalog_name}_q02
        SELECT * FROM all_partition_types1_parquet_snappy_src where id = 3;
        """
        order_qt_q04 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q02;
        """

        sql """
        INSERT OVERWRITE TABLE all_partition_types1_${format_compression}_${catalog_name}_q02
        SELECT CAST(7 as INT) as id, boolean_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col FROM all_partition_types1_parquet_snappy_src where id = 3;
        """
        order_qt_q05 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q02;
        """

        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q02; """)
        hive_docker """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q02; """
    }

    def q03 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_partition_types2_${format_compression}_${catalog_name}_q03; """)
        hive_docker """ DROP TABLE IF EXISTS all_partition_types2_${format_compression}_${catalog_name}_q03; """
        logger.info("hive sql: " + """ CREATE TABLE IF NOT EXISTS all_partition_types2_${format_compression}_${catalog_name}_q03 like all_partition_types2_${format_compression}; """)
        hive_docker """ CREATE TABLE IF NOT EXISTS all_partition_types2_${format_compression}_${catalog_name}_q03 like all_partition_types2_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_partition_types2_${format_compression}_${catalog_name}_q03
        SELECT * FROM all_partition_types2_parquet_snappy_src where id = 1;
        """
        order_qt_q01 """ select * from all_partition_types2_${format_compression}_${catalog_name}_q03;
        """

        sql """
        INSERT INTO all_partition_types2_${format_compression}_${catalog_name}_q03
        SELECT * FROM all_partition_types2_parquet_snappy_src where id = 2;
        """
        order_qt_q02 """ select * from all_partition_types2_${format_compression}_${catalog_name}_q03;
        """

        sql """
        INSERT OVERWRITE TABLE all_partition_types2_${format_compression}_${catalog_name}_q03
        SELECT * FROM all_partition_types2_parquet_snappy_src;
        """
        order_qt_q03 """ select * from all_partition_types2_${format_compression}_${catalog_name}_q03;
        """

        sql """
        INSERT INTO all_partition_types2_${format_compression}_${catalog_name}_q03
        SELECT * FROM all_partition_types2_parquet_snappy_src where id = 3;
        """
        order_qt_q04 """ select * from all_partition_types2_${format_compression}_${catalog_name}_q03;
        """

        sql """
        INSERT OVERWRITE TABLE all_partition_types2_${format_compression}_${catalog_name}_q03
        SELECT CAST(7 as INT) as id, decimal_col, string_col, date_col, char_col, varchar_col FROM all_partition_types2_parquet_snappy_src where id = 3;
        """
        order_qt_q05 """ select * from all_partition_types2_${format_compression}_${catalog_name}_q03;
        """

        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_partition_types2_${format_compression}_${catalog_name}_q03; """)
        hive_docker """ DROP TABLE IF EXISTS all_partition_types2_${format_compression}_${catalog_name}_q03; """
    }

    def q04 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q04; """)
        hive_docker """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q04; """
        logger.info("hive sql: " + """ CREATE TABLE IF NOT EXISTS all_partition_types1_${format_compression}_${catalog_name}_q04 like all_partition_types1_${format_compression}; """)
        hive_docker """ CREATE TABLE IF NOT EXISTS all_partition_types1_${format_compression}_${catalog_name}_q04 like all_partition_types1_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_partition_types1_${format_compression}_${catalog_name}_q04
        SELECT id, null, tinyint_col, null, int_col, null, float_col, null FROM all_partition_types1_parquet_snappy_src where id = 1;
        """
        order_qt_q01 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q04;
        """

        sql """
        INSERT INTO all_partition_types1_${format_compression}_${catalog_name}_q04
        SELECT id, null, tinyint_col, null, int_col, null, float_col, null FROM all_partition_types1_parquet_snappy_src where id = 2;
        """
        order_qt_q02 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q04;
        """

        sql """
        INSERT OVERWRITE TABLE all_partition_types1_${format_compression}_${catalog_name}_q04
        SELECT id, null, tinyint_col, null, int_col, null, float_col, null FROM all_partition_types1_parquet_snappy_src;
        """
        order_qt_q03 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q04;
        """

        sql """
        INSERT INTO all_partition_types1_${format_compression}_${catalog_name}_q04
        SELECT id, null, tinyint_col, null, int_col, null, float_col, null FROM all_partition_types1_parquet_snappy_src where id = 3;
        """
        order_qt_q04 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q04;
        """

        sql """
        INSERT OVERWRITE TABLE all_partition_types1_${format_compression}_${catalog_name}_q04
        SELECT CAST(7 as INT) as id, boolean_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col FROM all_partition_types1_parquet_snappy_src where id = 3;
        """
        order_qt_q05 """ select * from all_partition_types1_${format_compression}_${catalog_name}_q04;
        """

        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q04; """)
        hive_docker """ DROP TABLE IF EXISTS all_partition_types1_${format_compression}_${catalog_name}_q04; """
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
            logger.info("hive sql: " + """ use `write_test` """)
            hive_docker """use `write_test`"""

            sql """set enable_fallback_to_original_planner=false;"""

            for (String format_compression in format_compressions) {
                logger.info("Process format_compression " + format_compression)
                q01(format_compression, catalog_name)
                q02(format_compression, catalog_name)
                q03(format_compression, catalog_name)
                q04(format_compression, catalog_name)
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}


