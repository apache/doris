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
    sql """ use test_db;""" 

    sql """ set enable_fallback_to_original_planner=false; """
        
    // Test table name
    String table_name = "iceberg_ddl_test"
        
    // Clean up existing table if exists
    sql """ drop table if exists ${table_name} """
        
    // Test 1: Create initial Iceberg table with basic schema
    sql """
    CREATE TABLE ${table_name} (
        id INT,
        name STRING,
        age INT,
        score DOUBLE
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
    order_qt_init_1 """ DESC ${table_name} """
    qt_init_2 """ SELECT * FROM ${table_name} ORDER BY id """

    // Test 2: ADD COLUMN - basic type
    sql """ ALTER TABLE ${table_name} ADD COLUMN email STRING """
        
    // Verify schema after adding column
    order_qt_add_1 """ DESC ${table_name} """
    
    // Insert data with new column (existing rows should have NULL for new column)
    sql """ INSERT INTO ${table_name} VALUES (4, 'David', 28, 89.1, 'david@example.com') """
    
    // Verify data with new column
    qt_add_2 """ SELECT * FROM ${table_name} ORDER BY id """
    qt_add_3 """ SELECT id, email FROM ${table_name} WHERE email IS NOT NULL ORDER BY id """
    qt_add_4 """ SELECT id, email FROM ${table_name} WHERE email IS NULL ORDER BY id """
        
    // Test 3: ADD complex type column
    sql """ ALTER TABLE ${table_name} ADD COLUMN address STRUCT<city: STRING, country: STRING> """
        
    order_qt_add_multi_1 """ DESC ${table_name} """
    
    // Insert data with all columns
    sql """ INSERT INTO ${table_name} VALUES (5, 'Eve', 26, 91.3, 'eve@example.com', STRUCT('New York', 'USA'))  """
    
    qt_add_multi_2 """ SELECT * FROM ${table_name} ORDER BY id """
    qt_add_multi_3 """ SELECT id, address FROM ${table_name} WHERE id = 5 ORDER BY id """
        
    // Test 4: RENAME COLUMN
    sql """ ALTER TABLE ${table_name} RENAME COLUMN score grade """
        
    // Verify column renamed
    order_qt_rename_1 """ DESC ${table_name} """
    qt_rename_2 """ SELECT id, grade FROM ${table_name} ORDER BY id """
    qt_rename_3 """ SELECT * FROM ${table_name} WHERE grade > 90 ORDER BY id """

    // Test 5: DROP COLUMN
    sql """ ALTER TABLE ${table_name} DROP COLUMN name """
        
    // Verify column dropped
    order_qt_drop_1 """ DESC ${table_name} """
    qt_drop_2 """ SELECT * FROM ${table_name} ORDER BY id """
    qt_drop_3 """ SELECT id, age FROM ${table_name} WHERE age > 25 ORDER BY id """
}
