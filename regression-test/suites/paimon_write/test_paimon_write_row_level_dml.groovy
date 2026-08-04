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

suite("test_paimon_write_row_level_dml", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_row_dml_catalog"
    String dbName = "test_pw_row_dml_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_dml;
        CREATE TABLE paimon.${dbName}.t_dml (
            id INT, name STRING, score INT, status STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_append_only;
        CREATE TABLE paimon.${dbName}.t_append_only (
            id INT, name STRING
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_first_row;
        CREATE TABLE paimon.${dbName}.t_first_row (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'first-row'
        );
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    sql """create database if not exists internal.${dbName}"""
    sql """drop table if exists internal.${dbName}.t_merge_source"""
    sql """
        CREATE TABLE internal.${dbName}.t_merge_source (
            id INT, name STRING, score INT, action STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """
    sql """INSERT INTO internal.${dbName}.t_merge_source VALUES
        (1, 'Alice_merged', 100, 'U'),
        (2, 'ignored', 0, 'D'),
        (5, 'Eve', 50, 'I')
    """

    try {
        sql """INSERT INTO t_dml VALUES
            (1, 'Alice', 10, 'active'),
            (2, 'Bob', 20, 'active'),
            (3, 'Charlie', 30, 'active'),
            (4, 'Diana', 40, 'active')
        """

        sql """
            UPDATE t_dml
            SET name = concat(name, '_updated'), score = score + 1
            WHERE id IN (1, 2)
        """
        order_qt_paimon_update """SELECT * FROM t_dml ORDER BY id"""

        sql """DELETE FROM t_dml WHERE id = 3"""
        order_qt_paimon_delete """SELECT * FROM t_dml ORDER BY id"""

        sql """
            MERGE INTO t_dml t
            USING internal.${dbName}.t_merge_source s
            ON t.id = s.id
            WHEN MATCHED AND s.action = 'D' THEN DELETE
            WHEN MATCHED THEN UPDATE SET
                name = s.name,
                score = s.score,
                status = 'merged'
            WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                VALUES (s.id, s.name, s.score, 'inserted')
        """
        order_qt_paimon_merge """SELECT * FROM t_dml ORDER BY id"""

        test {
            sql """UPDATE t_append_only SET name = 'x' WHERE id = 1"""
            exception "Paimon UPDATE requires a primary-key table"
        }
        test {
            sql """DELETE FROM t_append_only WHERE id = 1"""
            exception "Paimon DELETE requires a primary-key table"
        }
        test {
            sql """UPDATE t_first_row SET name = 'x' WHERE id = 1"""
            exception "Paimon UPDATE does not support merge-engine=first-row"
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
        sql """drop table if exists internal.${dbName}.t_merge_source"""
    }
}
