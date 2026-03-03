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

suite("test_iceberg_table_cache", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    String catalogWithCache = "test_iceberg_table_cache_with_cache"
    String catalogNoCache = "test_iceberg_table_cache_no_cache"
    String testDb = "cache_test_db"

    // Create catalogs
    sql """drop catalog if exists ${catalogWithCache}"""
    sql """drop catalog if exists ${catalogNoCache}"""

    // Catalog with cache enabled (default)
    sql """
        CREATE CATALOG ${catalogWithCache} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        );
    """

    // Catalog with cache disabled (TTL=0)
    sql """
        CREATE CATALOG ${catalogNoCache} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0"
        );
    """

    try {
        // Create test database via Spark
        spark_iceberg "CREATE DATABASE IF NOT EXISTS demo.${testDb}"

        // ==================== Test 1: DML Operations ====================
        logger.info("========== Test 1: DML Operations ==========")

        // Test 1.1: INSERT
        logger.info("--- Test 1.1: External INSERT ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_insert"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_insert (id INT, name STRING) USING iceberg"
        spark_iceberg "INSERT INTO demo.${testDb}.test_insert VALUES (1, 'initial')"

        // Query from Doris to cache the data
        sql """switch ${catalogWithCache}"""
        def result1 = sql """select * from ${testDb}.test_insert order by id"""
        logger.info("Initial data (with cache): ${result1}")
        assertEquals(1, result1.size())

        sql """switch ${catalogNoCache}"""
        def result1_no_cache = sql """select * from ${testDb}.test_insert order by id"""
        logger.info("Initial data (no cache): ${result1_no_cache}")
        assertEquals(1, result1_no_cache.size())

        // External INSERT via Spark
        spark_iceberg "INSERT INTO demo.${testDb}.test_insert VALUES (2, 'external_insert')"

        // Query without refresh - cached catalog should see old data
        sql """switch ${catalogWithCache}"""
        def result2 = sql """select * from ${testDb}.test_insert order by id"""
        logger.info("After external INSERT (with cache, no refresh): ${result2}")
        assertEquals(1, result2.size())  // Should still see 1 row due to cache

        // Query without refresh - no-cache catalog should see new data
        sql """switch ${catalogNoCache}"""
        def result2_no_cache = sql """select * from ${testDb}.test_insert order by id"""
        logger.info("After external INSERT (no cache): ${result2_no_cache}")
        assertEquals(2, result2_no_cache.size())  // Should see 2 rows

        // Refresh and verify
        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_insert"""
        def result3 = sql """select * from ${testDb}.test_insert order by id"""
        logger.info("After REFRESH TABLE (with cache): ${result3}")
        assertEquals(2, result3.size())  // Should now see 2 rows

        // Test 1.2: DELETE
        logger.info("--- Test 1.2: External DELETE ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_delete"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_delete (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version'='2')"
        spark_iceberg "INSERT INTO demo.${testDb}.test_delete VALUES (1, 'row1'), (2, 'row2'), (3, 'row3')"

        // Cache the data
        sql """switch ${catalogWithCache}"""
        def del_result1 = sql """select * from ${testDb}.test_delete order by id"""
        assertEquals(3, del_result1.size())

        sql """switch ${catalogNoCache}"""
        def del_result1_nc = sql """select * from ${testDb}.test_delete order by id"""
        assertEquals(3, del_result1_nc.size())

        // External DELETE via Spark
        spark_iceberg "DELETE FROM demo.${testDb}.test_delete WHERE id = 2"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def del_result2 = sql """select * from ${testDb}.test_delete order by id"""
        logger.info("After external DELETE (with cache, no refresh): ${del_result2}")
        assertEquals(3, del_result2.size())  // Should still see 3 rows

        sql """switch ${catalogNoCache}"""
        def del_result2_nc = sql """select * from ${testDb}.test_delete order by id"""
        logger.info("After external DELETE (no cache): ${del_result2_nc}")
        assertEquals(2, del_result2_nc.size())  // Should see 2 rows

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_delete"""
        def del_result3 = sql """select * from ${testDb}.test_delete order by id"""
        assertEquals(2, del_result3.size())

        // Test 1.3: UPDATE
        logger.info("--- Test 1.3: External UPDATE ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_update"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_update (id INT, value INT) USING iceberg TBLPROPERTIES ('format-version'='2')"
        spark_iceberg "INSERT INTO demo.${testDb}.test_update VALUES (1, 100), (2, 200)"

        // Cache the data
        sql """switch ${catalogWithCache}"""
        def upd_result1 = sql """select * from ${testDb}.test_update order by id"""
        assertEquals(2, upd_result1.size())
        assertEquals(100, upd_result1[0][1])

        // External UPDATE via Spark
        spark_iceberg "UPDATE demo.${testDb}.test_update SET value = 999 WHERE id = 1"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def upd_result2 = sql """select * from ${testDb}.test_update order by id"""
        logger.info("After external UPDATE (with cache, no refresh): ${upd_result2}")
        assertEquals(100, upd_result2[0][1])  // Should still see old value

        sql """switch ${catalogNoCache}"""
        def upd_result2_nc = sql """select * from ${testDb}.test_update order by id"""
        logger.info("After external UPDATE (no cache): ${upd_result2_nc}")
        assertEquals(999, upd_result2_nc[0][1])  // Should see new value

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_update"""
        def upd_result3 = sql """select * from ${testDb}.test_update order by id"""
        assertEquals(999, upd_result3[0][1])

        // Test 1.4: INSERT OVERWRITE
        logger.info("--- Test 1.4: External INSERT OVERWRITE ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_overwrite"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_overwrite (id INT, name STRING) USING iceberg"
        spark_iceberg "INSERT INTO demo.${testDb}.test_overwrite VALUES (1, 'old1'), (2, 'old2')"

        // Cache the data
        sql """switch ${catalogWithCache}"""
        def ow_result1 = sql """select * from ${testDb}.test_overwrite order by id"""
        assertEquals(2, ow_result1.size())

        // External INSERT OVERWRITE via Spark
        spark_iceberg "INSERT OVERWRITE demo.${testDb}.test_overwrite SELECT 10, 'new'"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def ow_result2 = sql """select * from ${testDb}.test_overwrite order by id"""
        logger.info("After external INSERT OVERWRITE (with cache, no refresh): ${ow_result2}")
        assertEquals(2, ow_result2.size())  // Should still see old data

        sql """switch ${catalogNoCache}"""
        def ow_result2_nc = sql """select * from ${testDb}.test_overwrite order by id"""
        logger.info("After external INSERT OVERWRITE (no cache): ${ow_result2_nc}")
        assertEquals(1, ow_result2_nc.size())  // Should see new data

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_overwrite"""
        def ow_result3 = sql """select * from ${testDb}.test_overwrite order by id"""
        assertEquals(1, ow_result3.size())

        // ==================== Test 2: Schema Change Operations ====================
        logger.info("========== Test 2: Schema Change Operations ==========")

        // Test 2.1: ADD COLUMN
        logger.info("--- Test 2.1: External ADD COLUMN ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_add_column"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_add_column (id INT, name STRING) USING iceberg"
        spark_iceberg "INSERT INTO demo.${testDb}.test_add_column VALUES (1, 'test')"

        // Cache the schema
        sql """switch ${catalogWithCache}"""
        def add_col_desc1 = sql """desc ${testDb}.test_add_column"""
        logger.info("Initial schema (with cache): ${add_col_desc1}")
        assertEquals(2, add_col_desc1.size())

        sql """switch ${catalogNoCache}"""
        def add_col_desc1_nc = sql """desc ${testDb}.test_add_column"""
        assertEquals(2, add_col_desc1_nc.size())

        // External ADD COLUMN via Spark
        spark_iceberg "ALTER TABLE demo.${testDb}.test_add_column ADD COLUMN new_col INT"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def add_col_desc2 = sql """desc ${testDb}.test_add_column"""
        logger.info("After external ADD COLUMN (with cache, no refresh): ${add_col_desc2}")
        assertEquals(2, add_col_desc2.size())  // Should still see 2 columns

        sql """switch ${catalogNoCache}"""
        def add_col_desc2_nc = sql """desc ${testDb}.test_add_column"""
        logger.info("After external ADD COLUMN (no cache): ${add_col_desc2_nc}")
        assertEquals(3, add_col_desc2_nc.size())  // Should see 3 columns

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_add_column"""
        def add_col_desc3 = sql """desc ${testDb}.test_add_column"""
        assertEquals(3, add_col_desc3.size())

        // Test 2.2: DROP COLUMN
        logger.info("--- Test 2.2: External DROP COLUMN ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_drop_column"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_drop_column (id INT, name STRING, to_drop INT) USING iceberg"
        spark_iceberg "INSERT INTO demo.${testDb}.test_drop_column VALUES (1, 'test', 100)"

        // Cache the schema
        sql """switch ${catalogWithCache}"""
        def drop_col_desc1 = sql """desc ${testDb}.test_drop_column"""
        assertEquals(3, drop_col_desc1.size())

        // External DROP COLUMN via Spark
        spark_iceberg "ALTER TABLE demo.${testDb}.test_drop_column DROP COLUMN to_drop"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def drop_col_desc2 = sql """desc ${testDb}.test_drop_column"""
        logger.info("After external DROP COLUMN (with cache, no refresh): ${drop_col_desc2}")
        assertEquals(3, drop_col_desc2.size())  // Should still see 3 columns

        sql """switch ${catalogNoCache}"""
        def drop_col_desc2_nc = sql """desc ${testDb}.test_drop_column"""
        logger.info("After external DROP COLUMN (no cache): ${drop_col_desc2_nc}")
        assertEquals(2, drop_col_desc2_nc.size())  // Should see 2 columns

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_drop_column"""
        def drop_col_desc3 = sql """desc ${testDb}.test_drop_column"""
        assertEquals(2, drop_col_desc3.size())

        // Test 2.3: RENAME COLUMN
        logger.info("--- Test 2.3: External RENAME COLUMN ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_rename_column"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_rename_column (id INT, old_name STRING) USING iceberg"
        spark_iceberg "INSERT INTO demo.${testDb}.test_rename_column VALUES (1, 'test')"

        // Cache the schema
        sql """switch ${catalogWithCache}"""
        def rename_col_desc1 = sql """desc ${testDb}.test_rename_column"""
        logger.info("Initial schema: ${rename_col_desc1}")
        assertTrue(rename_col_desc1.toString().contains("old_name"))

        // External RENAME COLUMN via Spark
        spark_iceberg "ALTER TABLE demo.${testDb}.test_rename_column RENAME COLUMN old_name TO new_name"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def rename_col_desc2 = sql """desc ${testDb}.test_rename_column"""
        logger.info("After external RENAME COLUMN (with cache, no refresh): ${rename_col_desc2}")
        assertTrue(rename_col_desc2.toString().contains("old_name"))  // Should still see old name

        sql """switch ${catalogNoCache}"""
        def rename_col_desc2_nc = sql """desc ${testDb}.test_rename_column"""
        logger.info("After external RENAME COLUMN (no cache): ${rename_col_desc2_nc}")
        assertTrue(rename_col_desc2_nc.toString().contains("new_name"))  // Should see new name

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_rename_column"""
        def rename_col_desc3 = sql """desc ${testDb}.test_rename_column"""
        assertTrue(rename_col_desc3.toString().contains("new_name"))

        // Test 2.4: ALTER COLUMN TYPE
        logger.info("--- Test 2.4: External ALTER COLUMN TYPE ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_alter_type"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_alter_type (id INT, value INT) USING iceberg"
        spark_iceberg "INSERT INTO demo.${testDb}.test_alter_type VALUES (1, 100)"

        // Cache the schema
        sql """switch ${catalogWithCache}"""
        def alter_type_desc1 = sql """desc ${testDb}.test_alter_type"""
        logger.info("Initial schema: ${alter_type_desc1}")
        // value column should be INT
        assertTrue(alter_type_desc1[1][1].toString().toLowerCase().contains("int"))

        // External ALTER COLUMN TYPE via Spark (INT -> BIGINT)
        spark_iceberg "ALTER TABLE demo.${testDb}.test_alter_type ALTER COLUMN value TYPE BIGINT"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def alter_type_desc2 = sql """desc ${testDb}.test_alter_type"""
        logger.info("After external ALTER TYPE (with cache, no refresh): ${alter_type_desc2}")
        assertTrue(alter_type_desc2[1][1].toString().toLowerCase().contains("int"))  // Should still see INT

        sql """switch ${catalogNoCache}"""
        def alter_type_desc2_nc = sql """desc ${testDb}.test_alter_type"""
        logger.info("After external ALTER TYPE (no cache): ${alter_type_desc2_nc}")
        assertTrue(alter_type_desc2_nc[1][1].toString().toLowerCase().contains("bigint"))  // Should see BIGINT

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_alter_type"""
        def alter_type_desc3 = sql """desc ${testDb}.test_alter_type"""
        assertTrue(alter_type_desc3[1][1].toString().toLowerCase().contains("bigint"))

        // ==================== Test 3: Partition Evolution ====================
        logger.info("========== Test 3: Partition Evolution ==========")

        // Test 3.1: ADD PARTITION FIELD
        logger.info("--- Test 3.1: External ADD PARTITION FIELD ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_add_partition"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_add_partition (id INT, dt DATE, value INT) USING iceberg"
        spark_iceberg "INSERT INTO demo.${testDb}.test_add_partition VALUES (1, DATE'2024-01-15', 100)"

        // Cache the partition spec by querying data (show partitions is not supported for Iceberg tables)
        sql """switch ${catalogWithCache}"""
        def add_part_result_initial = sql """select count(*) from ${testDb}.test_add_partition"""
        logger.info("Initial data count (with cache): ${add_part_result_initial}")

        // External ADD PARTITION FIELD via Spark
        spark_iceberg "ALTER TABLE demo.${testDb}.test_add_partition ADD PARTITION FIELD month(dt)"

        // Insert data after partition evolution
        spark_iceberg "INSERT INTO demo.${testDb}.test_add_partition VALUES (2, DATE'2024-02-20', 200)"

        // Verify cache behavior - check data count as partition spec is harder to verify directly
        sql """switch ${catalogWithCache}"""
        def add_part_result1 = sql """select count(*) from ${testDb}.test_add_partition"""
        logger.info("After external ADD PARTITION FIELD (with cache, no refresh): ${add_part_result1}")
        assertEquals(1, add_part_result1[0][0])  // Should still see 1 row

        sql """switch ${catalogNoCache}"""
        def add_part_result1_nc = sql """select count(*) from ${testDb}.test_add_partition"""
        logger.info("After external ADD PARTITION FIELD (no cache): ${add_part_result1_nc}")
        assertEquals(2, add_part_result1_nc[0][0])  // Should see 2 rows

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_add_partition"""
        def add_part_result2 = sql """select count(*) from ${testDb}.test_add_partition"""
        assertEquals(2, add_part_result2[0][0])

        // Test 3.2: DROP PARTITION FIELD
        logger.info("--- Test 3.2: External DROP PARTITION FIELD ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_drop_partition"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_drop_partition (id INT, category STRING, value INT) USING iceberg PARTITIONED BY (category)"
        spark_iceberg "INSERT INTO demo.${testDb}.test_drop_partition VALUES (1, 'A', 100), (2, 'B', 200)"

        // Cache the partition spec
        sql """switch ${catalogWithCache}"""
        def drop_part_result1 = sql """select * from ${testDb}.test_drop_partition order by id"""
        assertEquals(2, drop_part_result1.size())

        // External DROP PARTITION FIELD via Spark
        spark_iceberg "ALTER TABLE demo.${testDb}.test_drop_partition DROP PARTITION FIELD category"

        // Insert data after partition evolution
        spark_iceberg "INSERT INTO demo.${testDb}.test_drop_partition VALUES (3, 'C', 300)"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def drop_part_result2 = sql """select count(*) from ${testDb}.test_drop_partition"""
        logger.info("After external DROP PARTITION FIELD (with cache, no refresh): ${drop_part_result2}")
        assertEquals(2, drop_part_result2[0][0])  // Should still see 2 rows

        sql """switch ${catalogNoCache}"""
        def drop_part_result2_nc = sql """select count(*) from ${testDb}.test_drop_partition"""
        logger.info("After external DROP PARTITION FIELD (no cache): ${drop_part_result2_nc}")
        assertEquals(3, drop_part_result2_nc[0][0])  // Should see 3 rows

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_drop_partition"""
        def drop_part_result3 = sql """select count(*) from ${testDb}.test_drop_partition"""
        assertEquals(3, drop_part_result3[0][0])

        // Test 3.3: REPLACE PARTITION FIELD
        logger.info("--- Test 3.3: External REPLACE PARTITION FIELD ---")
        spark_iceberg "DROP TABLE IF EXISTS demo.${testDb}.test_replace_partition"
        spark_iceberg "CREATE TABLE demo.${testDb}.test_replace_partition (id INT, ts TIMESTAMP, value INT) USING iceberg PARTITIONED BY (days(ts))"
        spark_iceberg "INSERT INTO demo.${testDb}.test_replace_partition VALUES (1, TIMESTAMP'2024-01-15 10:00:00', 100)"

        // Cache the partition spec
        sql """switch ${catalogWithCache}"""
        def replace_part_result1 = sql """select * from ${testDb}.test_replace_partition order by id"""
        assertEquals(1, replace_part_result1.size())

        // External REPLACE PARTITION FIELD via Spark (days -> months)
        spark_iceberg "ALTER TABLE demo.${testDb}.test_replace_partition REPLACE PARTITION FIELD days(ts) WITH months(ts)"

        // Insert data after partition evolution
        spark_iceberg "INSERT INTO demo.${testDb}.test_replace_partition VALUES (2, TIMESTAMP'2024-02-20 15:00:00', 200)"

        // Verify cache behavior
        sql """switch ${catalogWithCache}"""
        def replace_part_result2 = sql """select count(*) from ${testDb}.test_replace_partition"""
        logger.info("After external REPLACE PARTITION FIELD (with cache, no refresh): ${replace_part_result2}")
        assertEquals(1, replace_part_result2[0][0])  // Should still see 1 row

        sql """switch ${catalogNoCache}"""
        def replace_part_result2_nc = sql """select count(*) from ${testDb}.test_replace_partition"""
        logger.info("After external REPLACE PARTITION FIELD (no cache): ${replace_part_result2_nc}")
        assertEquals(2, replace_part_result2_nc[0][0])  // Should see 2 rows

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_replace_partition"""
        def replace_part_result3 = sql """select count(*) from ${testDb}.test_replace_partition"""
        assertEquals(2, replace_part_result3[0][0])

        logger.info("All tests passed!")

    } finally {
    }
}
