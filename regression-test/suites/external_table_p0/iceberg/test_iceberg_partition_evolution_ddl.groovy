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

suite("test_iceberg_partition_evolution_ddl", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_partition_evolution_ddl"

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

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """drop database if exists test_partition_evolution_db force"""
    sql """create database test_partition_evolution_db"""
    sql """use test_partition_evolution_db;"""

    sql """set enable_fallback_to_original_planner=false;"""

    // Test 1: Add identity partition field
    String table1 = "test_add_identity_partition"
    sql """drop table if exists ${table1}"""
    sql """
    CREATE TABLE ${table1} (
        id INT,
        name STRING,
        category STRING
    );
    """
    sql """INSERT INTO ${table1} VALUES (1, 'Alice', 'A'), (2, 'Bob', 'B')"""
    
    sql """ALTER TABLE ${table1} ADD PARTITION KEY category"""
    // Insert data after adding partition field to see new partition info
    sql """INSERT INTO ${table1} VALUES (3, 'Charlie', 'C')"""
    qt_add_identity_2 """SELECT * FROM ${table1} ORDER BY id"""
    order_qt_partitions_after_1 """SELECT `partition`, spec_id, record_count FROM ${table1}\$partitions ORDER BY `partition`"""

    // Test 2: Add time-based partition fields
    String table2 = "test_add_time_partition"
    sql """drop table if exists ${table2}"""
    sql """
    CREATE TABLE ${table2} (
        id INT,
        name STRING,
        ts DATETIME
    );
    """
    sql """INSERT INTO ${table2} VALUES (1, 'Alice', '2024-01-01 10:00:00'), (2, 'Bob', '2024-02-01 11:00:00')"""
    
    sql """ALTER TABLE ${table2} ADD PARTITION KEY year(ts)"""
    // Insert data after adding partition field to see new partition info
    sql """INSERT INTO ${table2} VALUES (3, 'Charlie', '2024-01-15 10:00:00')"""
    order_qt_partitions_year """SELECT `partition`, spec_id, record_count FROM ${table2}\$partitions ORDER BY `partition`"""
    
    sql """ALTER TABLE ${table2} ADD PARTITION KEY month(ts)"""
    // Insert data after adding partition field to see new partition info
    sql """INSERT INTO ${table2} VALUES (4, 'David', '2024-03-01 12:00:00')"""
    order_qt_partitions_month """SELECT `partition`, spec_id, record_count FROM ${table2}\$partitions ORDER BY `partition`"""
    
    qt_add_time_1 """SELECT * FROM ${table2} ORDER BY id"""

    // Test 3: Add bucket partition field
    String table3 = "test_add_bucket_partition"
    sql """drop table if exists ${table3}"""
    sql """
    CREATE TABLE ${table3} (
        id INT,
        name STRING,
        value DOUBLE
    );
    """
    sql """INSERT INTO ${table3} VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)"""
    
    sql """ALTER TABLE ${table3} ADD PARTITION KEY bucket(16, id)"""
    // Insert data after adding partition field to see new partition info
    sql """INSERT INTO ${table3} VALUES (3, 'Charlie', 300.0)"""
    qt_add_bucket_2 """SELECT * FROM ${table3} ORDER BY id"""
    order_qt_partitions_bucket """SELECT `partition`, spec_id, record_count FROM ${table3}\$partitions ORDER BY `partition`"""

    // Test 4: Add truncate partition field
    String table4 = "test_add_truncate_partition"
    sql """drop table if exists ${table4}"""
    sql """
    CREATE TABLE ${table4} (
        id INT,
        name STRING,
        code STRING
    );
    """
    sql """INSERT INTO ${table4} VALUES (1, 'Alice', 'ABCDE'), (2, 'Bob', 'FGHIJ')"""
    
    sql """ALTER TABLE ${table4} ADD PARTITION KEY truncate(5, code)"""
    // Insert data after adding partition field to see new partition info
    sql """INSERT INTO ${table4} VALUES (3, 'Charlie', 'KLMNO')"""
    qt_add_truncate_2 """SELECT * FROM ${table4} ORDER BY id"""
    order_qt_partitions_truncate """SELECT `partition`, spec_id, record_count FROM ${table4}\$partitions ORDER BY `partition`"""

    // Test 5: Drop partition field - identity
    String table5 = "test_drop_identity_partition"
    sql """drop table if exists ${table5}"""
    sql """
    CREATE TABLE ${table5} (
        id INT,
        name STRING,
        category STRING
    )
    PARTITION BY LIST (category) ();
    """
    sql """INSERT INTO ${table5} VALUES (1, 'Alice', 'A'), (2, 'Bob', 'B')"""

    // Drop partition field
    sql """ALTER TABLE ${table5} DROP PARTITION KEY category"""
    // Insert data after dropping partition field to see new partition info
    sql """INSERT INTO ${table5} VALUES (3, 'Charlie', 'C')"""
    qt_drop_identity_2 """SELECT * FROM ${table5} ORDER BY id"""
    order_qt_partitions_after_drop """SELECT `partition`, spec_id, record_count FROM ${table5}\$partitions ORDER BY `partition`"""

    // Test 6: Drop partition field - time-based
    String table6 = "test_drop_time_partition"
    sql """drop table if exists ${table6}"""
    sql """
    CREATE TABLE ${table6} (
        id INT,
        name STRING,
        created_date DATE
    )
    PARTITION BY LIST (year(created_date)) ();
    """
    sql """INSERT INTO ${table6} VALUES (1, 'Alice', '2023-01-01'), (2, 'Bob', '2024-01-01')"""
    
    sql """ALTER TABLE ${table6} DROP PARTITION KEY year(created_date)"""
    qt_drop_time_1 """DESC ${table6}"""
    
    sql """INSERT INTO ${table6} VALUES (3, 'Charlie', '2025-01-01')"""
    qt_drop_time_2 """SELECT * FROM ${table6} ORDER BY id"""


    // Test 7: Drop partition field - bucket
    String table7 = "test_drop_bucket_partition"
    sql """drop table if exists ${table7}"""
    sql """
    CREATE TABLE ${table7} (
        id INT,
        name STRING,
        value DOUBLE
    )
    PARTITION BY LIST (bucket(16, id)) ();
    """
    sql """INSERT INTO ${table7} VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)"""
    
    sql """ALTER TABLE ${table7} DROP PARTITION KEY bucket(16, id)"""
    qt_drop_bucket_1 """DESC ${table7}"""
    
    sql """INSERT INTO ${table7} VALUES (3, 'Charlie', 300.0)"""
    qt_drop_bucket_2 """SELECT * FROM ${table7} ORDER BY id"""

    // Test 8: Multiple partition evolution operations
    String table8 = "test_multiple_evolution"
    sql """drop table if exists ${table8}"""
    sql """
    CREATE TABLE ${table8} (
        id INT,
        name STRING,
        ts DATETIME,
        category STRING
    );
    """
    sql """INSERT INTO ${table8} VALUES (1, 'Alice', '2024-01-01 10:00:00', 'A')"""
    
    // Add multiple partition fields
    sql """ALTER TABLE ${table8} ADD PARTITION KEY day(ts)"""
    sql """ALTER TABLE ${table8} ADD PARTITION KEY category"""
    sql """ALTER TABLE ${table8} ADD PARTITION KEY bucket(8, id)"""
    // Insert data after adding partition fields to see new partition info
    sql """INSERT INTO ${table8} VALUES (2, 'Bob', '2024-02-01 11:00:00', 'B')"""
    qt_multiple_2 """SELECT * FROM ${table8} ORDER BY id"""
    order_qt_partitions_multiple_add """SELECT `partition`, spec_id, record_count FROM ${table8}\$partitions ORDER BY `partition`"""
    
    // Drop some partition fields
    sql """ALTER TABLE ${table8} DROP PARTITION KEY bucket(8, id)"""
    sql """ALTER TABLE ${table8} DROP PARTITION KEY category"""
    // Insert data after dropping partition fields to see new partition info
    sql """INSERT INTO ${table8} VALUES (3, 'Charlie', '2024-03-01 12:00:00', 'C')"""
    qt_multiple_4 """SELECT * FROM ${table8} ORDER BY id"""
    order_qt_partitions_multiple_after_drop """SELECT `partition`, spec_id, record_count FROM ${table8}\$partitions ORDER BY `partition`"""

    // Test 9: Error cases - drop non-existent partition field
    String table9 = "test_error_cases"
    sql """drop table if exists ${table9}"""
    sql """
    CREATE TABLE ${table9} (
        id INT,
        name STRING
    );
    """
    
    test {
        sql """ALTER TABLE ${table9} DROP PARTITION KEY bucket(16, id)"""
        exception "Cannot find partition field to remove"
    }

    // Test 10: Error cases - invalid transform
    test {
        sql """ALTER TABLE ${table9} ADD PARTITION KEY invalid_transform(id)"""
        exception "Unsupported partition transform"
    }

    // Test 11: Error cases - missing argument for bucket
    test {
        sql """ALTER TABLE ${table9} ADD PARTITION KEY bucket(id)"""
        exception "Bucket transform requires a bucket count argument"
    }

    // Test 12: Error cases - not an Iceberg table
    sql """create database if not exists internal.test_internal_table_db"""
    sql """drop table if exists internal.test_internal_table_db.test_internal_table"""
    sql """
    CREATE TABLE internal.test_internal_table_db.test_internal_table (
        id INT,
        name STRING
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """

    test {
        sql """ALTER TABLE internal.test_internal_table_db.test_internal_table ADD PARTITION KEY id"""
        exception "ADD PARTITION KEY is only supported for Iceberg tables"
    }

    // Test 13: Add partition field with AS key name and drop by key name
    String table13 = "test_add_partition_key_with_alias"
    sql """drop table if exists ${table13}"""
    sql """
    CREATE TABLE ${table13} (
        id INT,
        ts DATETIME
    );
    """
    sql """INSERT INTO ${table13} VALUES (1, '2024-01-01 08:00:00'), (2, '2024-02-02 09:00:00')"""

    sql """ALTER TABLE ${table13} ADD PARTITION KEY year(ts) AS ts_year"""
    sql """INSERT INTO ${table13} VALUES (3, '2025-03-03 10:00:00')"""
    qt_add_partition_key_alias """SELECT * FROM ${table13} ORDER BY id"""
    order_qt_add_partition_key_alias_partitions """SELECT `partition`, spec_id, record_count FROM ${table13}\$partitions ORDER BY `partition`"""

    // drop by custom key name
    sql """ALTER TABLE ${table13} DROP PARTITION KEY ts_year"""
    sql """INSERT INTO ${table13} VALUES (4, '2026-04-04 11:00:00')"""
    qt_drop_partition_key_alias """SELECT * FROM ${table13} ORDER BY id"""

    // Test 14: Replace partition field with/without AS key name
    String table14 = "test_replace_partition_field"
    sql """drop table if exists ${table14}"""
    sql """
    CREATE TABLE ${table14} (
        id INT,
        ts DATETIME
    );
    """
    sql """INSERT INTO ${table14} VALUES (1, '2024-01-01 00:00:00'), (2, '2024-02-01 00:00:00')"""

    sql """ALTER TABLE ${table14} ADD PARTITION KEY day(ts) AS ts_day"""
    sql """INSERT INTO ${table14} VALUES (3, '2024-01-05 00:00:00')"""

    // Replace without AS (key name becomes default transform name)
    sql """ALTER TABLE ${table14} REPLACE PARTITION KEY ts_day WITH month(ts)"""
    sql """INSERT INTO ${table14} VALUES (4, '2024-03-15 00:00:00')"""

    // Replace with AS to specify new name explicitly
    sql """ALTER TABLE ${table14} REPLACE PARTITION KEY ts_month WITH year(ts) AS ts_year"""
    sql """INSERT INTO ${table14} VALUES (5, '2025-04-20 00:00:00')"""

    qt_replace_partition_key_data """SELECT * FROM ${table14} ORDER BY id"""
    order_qt_replace_partition_key_partitions """SELECT `partition`, spec_id, record_count FROM ${table14}\$partitions ORDER BY `partition`"""
}
