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

suite("test_paimon_ddl", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Paimon test is not enabled, skip this test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_paimon_ddl"
    String dbName = "test_paimon_ddl_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """SWITCH ${catalogName}"""
    sql """DROP DATABASE IF EXISTS ${dbName} FORCE"""
    sql """CREATE DATABASE ${dbName}"""
    sql """USE ${dbName}"""

    sql """DROP TABLE IF EXISTS paimon_alter_table_renamed"""
    sql """DROP TABLE IF EXISTS paimon_alter_table"""
    sql """
        CREATE TABLE paimon_alter_table (
            id INT,
            name STRING
        ) ENGINE=paimon
        PROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2'
        )
    """

    sql """ALTER TABLE paimon_alter_table ADD COLUMN age INT FIRST"""
    sql """ALTER TABLE paimon_alter_table ADD COLUMN city STRING AFTER name"""
    sql """ALTER TABLE paimon_alter_table ADD COLUMN (score DOUBLE, remark STRING)"""
    sql """ALTER TABLE paimon_alter_table RENAME COLUMN city location"""
    sql """ALTER TABLE paimon_alter_table DROP COLUMN score"""
    sql """ALTER TABLE paimon_alter_table SET ('snapshot.num-retained.min' = '3')"""
    sql """ALTER TABLE paimon_alter_table RENAME TO paimon_alter_table_renamed"""
    sql """ALTER TABLE paimon_alter_table_renamed SET ('snapshot.num-retained.max' = '10')"""
}
