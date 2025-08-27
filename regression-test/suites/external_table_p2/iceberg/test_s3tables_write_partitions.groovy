import java.util.concurrent.ThreadLocalRandom

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

suite("test_s3tables_write_partitions", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    def format_compressions = ["parquet_snappy", "orc_zlib"]

    def test_s3_columns_out_of_order = {  String format_compression, String catalog_name ->
        def parts = format_compression.split("_")
        def format = parts[0]
        def compression = parts[1]
        def source_tbl = "s3_columns_out_of_order_source_tbl_${format_compression}_master"+ ThreadLocalRandom.current().nextInt(1000)
        def target_tbl = "s3_columns_out_of_order_target_tbl_${format_compression}_master" + ThreadLocalRandom.current().nextInt(1000)
        sql """ drop table if exists ${source_tbl} """
        sql """
            CREATE TABLE ${source_tbl} (
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
        sql """ drop table if exists ${target_tbl}"""
        sql """
            CREATE TABLE ${target_tbl} (
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
            INSERT INTO ${source_tbl} (
              col1, col2, col3, col4, col5, col6
            ) VALUES (1, 2, 3, 4, 5, 6);
            """
        order_qt_columns_out_of_order01 """ SELECT * FROM ${source_tbl} """

        sql """
            INSERT INTO ${target_tbl} (
              col1, col2, col3, col4, col5, col6
            ) VALUES (1, 2, 3, 4, 5, 6);
            """

        order_qt_columns_out_of_order02 """ SELECT * FROM ${target_tbl} """

        sql """ drop table ${source_tbl} """
        sql """ drop table ${target_tbl} """
        sql """ drop database if exists `test_s3_columns_out_of_order` """;
    }

    String enabled = context.config.otherConfigs.get("enableExternalIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String catalog_name = "test_s3tables_write_partitions"
    String props = context.config.otherConfigs.get("icebergS3TablesCatalog")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog ${catalog_name} properties (
            ${props}
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ use my_namespace;""" 
    sql """ set enable_fallback_to_original_planner=false """
    // def tables = sql """ show tables; """
    // assertTrue(tables.size() > 0)

    try {
        for (String format_compression in format_compressions) {
            logger.info("Process format_compression " + format_compression)
            test_s3_columns_out_of_order(format_compression, catalog_name)
        }
    } finally {
    }

}
