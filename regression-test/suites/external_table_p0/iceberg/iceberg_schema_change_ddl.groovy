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

suite("iceberg_schema_change_ddl", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_schema_change_ddl"

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

    sql """switch ${catalog_name};"""
    sql """drop database if exists iceberg_schema_change_ddl_db force"""
    sql """create database iceberg_schema_change_ddl_db"""
    sql """ use iceberg_schema_change_ddl_db;""" 

    sql """ set enable_fallback_to_original_planner=false; """
    sql """set show_column_comment_in_describe=true;"""
        
    // Test table name
    String table_name = "iceberg_ddl_test"
    String partition_table_name = "iceberg_ddl_partition_test"
        
    // Clean up existing table if exists
    sql """ drop table if exists ${table_name} """
    sql """ drop table if exists ${partition_table_name} """
        
    // Test 1: Create initial Iceberg table with basic schema
    sql """
    CREATE TABLE ${table_name} (
        id INT not null,
        name STRING not null,
        age INT not null,
        score DOUBLE not null
    );
    """
    
    // Insert initial data
    sql """
    INSERT INTO ${table_name} VALUES 
    (1, 'Alice', 25, 95.5),
    (2, 'Bob', 30, 87.2),
    (3, 'Charlie', 22, 92.8)
    """
        
    // Verify initial state
    qt_init_1 """ DESC ${table_name} """
    qt_init_2 """ SELECT * FROM ${table_name} ORDER BY id """

    // Test 2: ADD COLUMN - basic type
    test {
        sql """ ALTER TABLE iceberg_ddl_test ADD COLUMN email STRING not null DEFAULT 'N/A'; """
        exception "can't add a non-nullable column to an Iceberg table"
    }
    sql """ ALTER TABLE ${table_name} ADD COLUMN email STRING """
    // not support initial default value for new column
    // This will throw an exception in Iceberg v2, but is allowed in v3
    test {
        sql """ ALTER TABLE ${table_name} ADD COLUMN phone STRING DEFAULT 'N/A' COMMENT 'User phone number' """
        exception "Invalid initial default for phone: non-null default (N/A) is not supported until v3"
    }
    // Add column with comment and set position after age
    sql """ ALTER TABLE ${table_name} ADD COLUMN phone STRING COMMENT 'User phone number' after age """
    // Verify schema after adding column
    qt_add_1 """ DESC ${table_name} """
    
    // Insert data with new column (existing rows should have NULL for new column)
    sql """ INSERT INTO ${table_name} VALUES (4, 'David', 28, '123-456-7890', 89.1, 'david@example.com') """
    // Verify data with new column
    qt_add_2 """ SELECT * FROM ${table_name} ORDER BY id """
    qt_add_3 """ SELECT id, email FROM ${table_name} WHERE email IS NOT NULL ORDER BY id """
    qt_add_4 """ SELECT id, email FROM ${table_name} WHERE email IS NULL ORDER BY id """
        
    // Test 3: ADD complex type column
    sql """ ALTER TABLE ${table_name} ADD COLUMN address STRUCT<city: STRING, country: STRING> """
        
    qt_add_multi_1 """ DESC ${table_name} """
    
    // Insert data with all columns
    sql """ INSERT INTO ${table_name} VALUES (5, 'Eve', 26, '223-345-132', 91.3, 'eve@example.com', STRUCT('New York', 'USA'))  """
    
    qt_add_multi_2 """ SELECT * FROM ${table_name} ORDER BY id """
    qt_add_multi_3 """ SELECT id, address FROM ${table_name} WHERE id = 5 ORDER BY id """

    // Invalid add column
    test {
        sql """ALTER TABLE ${table_name} ADD COLUMN invalid_col1 int sum"""
        exception "Can not specify aggregation method for iceberg table column"
    }
    test {
        sql """ALTER TABLE ${table_name} ADD COLUMN invalid_col1 int AUTO_INCREMENT"""
        exception "Can not specify auto incremental iceberg table column"
    }
        
    // Test 4: RENAME COLUMN
    sql """ ALTER TABLE ${table_name} RENAME COLUMN score grade """
        
    // Verify column renamed
    qt_rename_1 """ DESC ${table_name} """
    qt_rename_2 """ SELECT id, grade FROM ${table_name} ORDER BY id """
    qt_rename_3 """ SELECT * FROM ${table_name} WHERE grade > 90 ORDER BY id """

    // Test 5: DROP COLUMN
    sql """ ALTER TABLE ${table_name} DROP COLUMN name """
        
    // Verify column dropped
    qt_drop_1 """ DESC ${table_name} """
    qt_drop_2 """ SELECT * FROM ${table_name} ORDER BY id """
    qt_drop_3 """ SELECT id, age FROM ${table_name} WHERE age > 25 ORDER BY id """

    // Test 6: ADD COLUMNS with default values
    sql """ ALTER TABLE ${table_name} ADD COLUMN (col1 FLOAT COMMENT 'User defined column1', col2 STRING COMMENT 'User defined column2') """

    // Verify new columns with default values
    qt_add_columns_1 """ DESC ${table_name} """
    qt_add_columns_2 """ SELECT id, col1, col2 FROM ${table_name} ORDER BY id """

    // invalid add columns
    test {
        sql """ALTER TABLE ${table_name} ADD COLUMN (col3 FLOAT COMMENT 'User defined column1', col4 int SUM COMMENT 'User defined column2')"""
        exception "Can not specify aggregation method for iceberg table column"
    }

    // Test 7: modify column type
    sql """ ALTER TABLE ${table_name} MODIFY COLUMN age BIGINT """
    // modify column type and add comment
    sql """ ALTER TABLE ${table_name} MODIFY COLUMN col1 DOUBLE COMMENT 'Updated column1 type' """
    // can't update column type by losing precision
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN age int """
        exception "Cannot change column type: age: long -> int"
    }
    // modify age to nullable and move to first position
    sql """ ALTER TABLE ${table_name} MODIFY COLUMN age bigint NULL FIRST"""
    sql """ insert into ${table_name} values (null, 6, '123-456-7890', 100.0, 'user6@example.com', STRUCT('Los Angeles', 'USA'), null, null)"""
    // Verify column type changed
    qt_modify_1 """ DESC ${table_name} """
    qt_modify_2 """ SELECT id, age, col1 FROM ${table_name} ORDER BY id """
    qt_modify_3 """ SELECT * FROM ${table_name} WHERE age > 25 ORDER BY id """
    // modify age to not null, which is not allowed
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN age bigint not null"""
        exception "Can not change nullable column age to not null"
    }
    // invalid modify
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN age bigint sum"""
        exception "Can not specify aggregation method for iceberg table column"
    }
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN age bigint AUTO_INCREMENT"""
        exception "Can not specify auto incremental iceberg table column"
    }
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN non_col bigint"""
        exception "Column non_col does not exist"
    }
    // not comment, the comment will be removed
    qt_before_no_comment "desc ${table_name}" 
    sql """ ALTER TABLE ${table_name} MODIFY COLUMN col1 DOUBLE"""
    qt_after_no_comment "desc ${table_name}"
    sql """ ALTER TABLE ${table_name} MODIFY COLUMN col1 DOUBLE COMMENT 'Updated column1 type'"""
    qt_after_no_comment "desc ${table_name}"

    // Test 7.1: More positive modify column type cases
    // Add test columns for type conversion tests
    sql """ ALTER TABLE ${table_name} ADD COLUMN test_float FLOAT """
    sql """ ALTER TABLE ${table_name} ADD COLUMN test_decimal DECIMAL(5,2) """
    sql """ INSERT INTO ${table_name} (id, test_float, test_decimal) VALUES (7, 3.14, 123.45) """
    
    // Positive case: float -> double
    sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_float DOUBLE """
    
    // Positive case: decimal precision expansion (scale unchanged)
    sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_decimal DECIMAL(10,2) """
    
    // Verify positive type changes
    qt_modify_positive_1 """ DESC ${table_name} """
    qt_modify_positive_2 """ SELECT id, test_float, test_decimal FROM ${table_name} WHERE id = 7 """

    // Test 7.2: Negative modify column type cases (should all fail)
    
    // double -> float (precision loss)
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN col1 FLOAT """
        exception "Cannot change column type"
    }
    
    // bigint -> int (precision loss) - already tested above but added here for completeness
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN age INT """
        exception "Cannot change column type"
    }
    
    // string -> numeric types
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN email INT """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN email DOUBLE """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN email DECIMAL(10,2) """
        exception "Cannot change column type"
    }
    
    // numeric -> string
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN id STRING """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_float STRING """
        exception "Cannot change column type"
    }
    
    // decimal precision reduction
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_decimal DECIMAL(3,2) """
        exception "Cannot change column type"
    }
    
    // decimal scale change (even if precision increases)
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_decimal DECIMAL(15,3) """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_decimal DECIMAL(15,1) """
        exception "Cannot change column type"
    }
    
    // int -> float/double (may lose precision for large integers)
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN id FLOAT """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN id DOUBLE """
        exception "Cannot change column type"
    }
    
    // float/double -> decimal
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_float DECIMAL(10,2) """
        exception "Cannot change column type"
    }
    
    // decimal -> float/double
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_decimal FLOAT """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN test_decimal DOUBLE """
        exception "Cannot change column type"
    }
    
    // struct/complex type changes
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN address STRING """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${table_name} MODIFY COLUMN email STRUCT<name: STRING> """
        exception "Modify column type to non-primitive type is not supported"
    }

    // Test 8: reorder columns
    sql """ ALTER TABLE ${table_name} ORDER BY (id, age, col1, col2, grade, phone, email, address) """
    // Verify column order changed
    qt_reorder_1 """ DESC ${table_name} """
    qt_reorder_2 """ SELECT * FROM ${table_name} ORDER BY id """

    // Test 9: Rename table
    String renamed_table_name = "iceberg_ddl_test_renamed"
    sql """ drop table if exists ${renamed_table_name} """
    // before rename
    qt_before_rename_show_tables_old """show tables like '${table_name}'"""
    qt_before_rename_show_tables_new """show tables like '${renamed_table_name}'"""
    sql """ ALTER TABLE ${table_name} RENAME ${renamed_table_name} """
    qt_after_rename_show_tables_old """show tables like '${table_name}'"""
    qt_after_rename_show_tables_new """show tables like '${renamed_table_name}'"""

    // Verify table renamed
    qt_rename_table_1 """ DESC ${renamed_table_name} """
    qt_rename_table_2 """ SELECT * FROM ${renamed_table_name} ORDER BY id """
    test {
        sql """ select * from ${table_name} """
        exception "Table [iceberg_ddl_test] does not exist in database [iceberg_schema_change_ddl_db]"
    }
    sql """ ALTER TABLE ${renamed_table_name} RENAME ${table_name} """
    qt_rename_table_back_1 """ SELECT * FROM ${table_name} ORDER BY id """
    
    // Test partitioned table schema change
    sql """ drop table if exists ${partition_table_name} """
    sql """ CREATE TABLE ${partition_table_name} (
        id INT,
        name STRING,
        age INT,
        score DOUBLE
    ) PARTITION BY LIST (name) ();"""
    // Insert initial data
    sql """ INSERT INTO ${partition_table_name} VALUES 
    (1, 'Alice', 25, 95.5),
    (2, 'Bob', 30, 87.2),
    (3, 'Charlie', 22, 92.8) """;
    // Verify initial state
    qt_partition_init_1 """ DESC ${partition_table_name} """
    qt_partition_init_2 """ SELECT * FROM ${partition_table_name} ORDER BY id """

    // can't drop partitioned column
    test {
        sql """ ALTER TABLE ${partition_table_name} DROP COLUMN name """
        exception "Failed to drop column"
    }

    // add new columns to partitioned table
    sql """ ALTER TABLE ${partition_table_name} ADD COLUMN email STRING """
    sql """ ALTER TABLE ${partition_table_name} ADD COLUMN phone STRING COMMENT 'User phone number' """

    // Verify schema after adding columns
    qt_partition_add_1 """ DESC ${partition_table_name} """

    // reorder columns in partitioned table
    sql """ ALTER TABLE ${partition_table_name} ORDER BY (id, age, email, phone, score, name) """
    // Verify column order changed
    qt_partition_reorder_1 """ DESC ${partition_table_name} """
    qt_partition_reorder_2 """ SELECT * FROM ${partition_table_name} ORDER BY id """

    // Test: Modify partition column type (should fail)
    // The 'name' column is the partition column for this table
    
    // Test modifying partition column type
    // no change, still string
    sql """ ALTER TABLE ${partition_table_name} MODIFY COLUMN name VARCHAR(100) """
    qt_partition_alter "desc ${partition_table_name}"

    // no change, still string
    sql """ ALTER TABLE ${partition_table_name} MODIFY COLUMN name TEXT """
    qt_partition_alter "desc ${partition_table_name}"
    
    test {
        sql """ ALTER TABLE ${partition_table_name} MODIFY COLUMN name INT """
        exception "Cannot change column type"
    }
    
    sql """ ALTER TABLE ${partition_table_name} MODIFY COLUMN name STRING FIRST """
    qt_reorder_partition_col """desc ${partition_table_name}"""
    
    // Test modifying partition column comment (might be allowed)
    sql """ ALTER TABLE ${partition_table_name} MODIFY COLUMN name STRING COMMENT 'Updated partition column comment' """
    qt_comment_partition_col """desc ${partition_table_name}"""
    
    // Create another partitioned table with integer partition column for more tests
    String int_partition_table_name = "iceberg_ddl_int_partition_test"
    sql """ drop table if exists ${int_partition_table_name} """
    sql """ CREATE TABLE ${int_partition_table_name} (
        id INT,
        name STRING,
        age INT,
        department_id INT
    ) PARTITION BY LIST (department_id) ();"""
    
    // Insert test data
    sql """ INSERT INTO ${int_partition_table_name} VALUES 
    (1, 'Alice', 25, 100),
    (2, 'Bob', 30, 200),
    (3, 'Charlie', 22, 100) """;
    
    // Test modifying integer partition column type
    sql """ ALTER TABLE ${int_partition_table_name} MODIFY COLUMN department_id BIGINT """
    qt_desc_partition_bigint "desc ${int_partition_table_name}"
    sql """ INSERT INTO ${int_partition_table_name} VALUES 
    (1, 'Alice', 25, 9223372036854775807),
    (2, 'Bob', 30, 9223372036854775806),
    (3, 'Charlie', 22, 9223372036854775805) """;
    order_qt_select_partition_bigint "select * from ${int_partition_table_name}"
    
    test {
        sql """ ALTER TABLE ${int_partition_table_name} MODIFY COLUMN department_id STRING """
        exception "Cannot change column type"
    }
    
    test {
        sql """ ALTER TABLE ${int_partition_table_name} MODIFY COLUMN department_id DOUBLE """
        exception "Cannot change column type"
    }
    
    // Verify that non-partition columns can still be modified in partitioned tables
    sql """ ALTER TABLE ${int_partition_table_name} MODIFY COLUMN age BIGINT """
    sql """ ALTER TABLE ${int_partition_table_name} MODIFY COLUMN name VARCHAR(200) COMMENT 'Updated name column' """
    
    // Verify the changes
    qt_partition_modify_1 """ DESC ${int_partition_table_name} """
    order_qt_partition_modify_2 """ SELECT * FROM ${int_partition_table_name} ORDER BY id """
    
    // Clean up
    sql """ drop table if exists ${int_partition_table_name} """
}

