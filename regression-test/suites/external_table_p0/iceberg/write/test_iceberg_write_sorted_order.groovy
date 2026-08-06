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

// Regression test for: BE SIGSEGV in MergeSorterState when writing to a
// sorted Iceberg table with data large enough to trigger multiple file flushes.
// MergeSorterState::reset() was not clearing _queue between flush cycles,
// leaving stale cursors that caused a null pointer dereference in
// update_batch_size() on the next flush.

suite("test_iceberg_write_sorted_order", "p0,external") {

    def test_sorted_write_multi_flush = { String catalog_name ->
        sql """ DROP TABLE IF EXISTS test_iceberg_sorted_write """
        sql """
            CREATE TABLE test_iceberg_sorted_write (
                id     INT,
                name   STRING,
                value  DOUBLE
            ) ENGINE = iceberg
            ORDER BY (id ASC)
            PROPERTIES (
                "compression-codec" = "zstd",
                "write-format" = "parquet"
            )
        """

        // Lower the target file size so each pipeline block (~90 KB) exceeds the
        // threshold, forcing _flush_to_file() → reset() on every write() call.
        // With 10 000 rows this produces 2+ flushes from write() — the minimum
        // needed to hit the stale-cursor crash on the second cycle.
        sql """ SET iceberg_write_target_file_size_bytes = 51200 """

        // 10 000 rows via the built-in numbers() generator — no external data needed.
        sql """
            INSERT INTO test_iceberg_sorted_write
            SELECT number,
                   concat('name_', cast(number AS STRING)),
                   number * 1.5
            FROM numbers("number" = "10000")
        """

        // Verify via Iceberg $files metadata table:
        // - multiple files were created (proves multiple flush cycles happened)
        // - total committed row count matches what was inserted
        def files = sql """ SELECT count(*), sum(record_count)
                            FROM test_iceberg_sorted_write\$files """
        def file_count  = files[0][0].toLong()
        def total_rows  = files[0][1].toLong()

        assertTrue(file_count > 1L,  "Expected multiple files from multi-flush write, got ${file_count}")
        assertEquals(10000L, total_rows)

        sql """ DROP TABLE IF EXISTS test_iceberg_sorted_write """
        sql """ SET iceberg_write_target_file_size_bytes = 0 """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port      = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port     = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String catalog_name  = "test_iceberg_sorted_write_${hivePrefix}"

            sql """ DROP CATALOG IF EXISTS ${catalog_name} """
            sql """
                CREATE CATALOG IF NOT EXISTS ${catalog_name} PROPERTIES (
                    'type'                 = 'iceberg',
                    'iceberg.catalog.type' = 'hms',
                    'hive.metastore.uris'  = 'thrift://${externalEnvIp}:${hms_port}',
                    'fs.defaultFS'         = 'hdfs://${externalEnvIp}:${hdfs_port}',
                    'use_meta_cache'       = 'true'
                )
            """

            sql """ DROP DATABASE IF EXISTS `${catalog_name}`.`sorted_write_test` """
            sql """ CREATE DATABASE `${catalog_name}`.`sorted_write_test` """
            sql """ USE `${catalog_name}`.`sorted_write_test` """

            sql """ SET enable_fallback_to_original_planner = false """

            test_sorted_write_multi_flush(catalog_name)

            sql """ DROP DATABASE IF EXISTS `${catalog_name}`.`sorted_write_test` """
            sql """ DROP CATALOG IF EXISTS ${catalog_name} """
        } finally {
        }
    }
}
