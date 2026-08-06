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

        DROP TABLE IF EXISTS paimon.${dbName}.t_ignore_delete;
        CREATE TABLE paimon.${dbName}.t_ignore_delete (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'ignore-delete' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_partial_update;
        CREATE TABLE paimon.${dbName}.t_partial_update (
            id INT, name STRING, score INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'partial-update',
            'partial-update.remove-record-on-delete' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_aggregation_no_delete;
        CREATE TABLE paimon.${dbName}.t_aggregation_no_delete (
            id INT, score INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'aggregation',
            'fields.score.aggregate-function' = 'sum'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_aggregation_delete;
        CREATE TABLE paimon.${dbName}.t_aggregation_delete (
            id INT, score INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'aggregation',
            'fields.score.aggregate-function' = 'sum',
            'aggregation.remove-record-on-delete' = 'true'
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

        sql """TRUNCATE TABLE internal.${dbName}.t_merge_source"""
        sql """INSERT INTO internal.${dbName}.t_merge_source VALUES
            (1, 'priority_update', 101, 'U'),
            (6, 'priority_insert', 60, 'I')
        """
        sql """
            MERGE INTO t_dml t
            USING internal.${dbName}.t_merge_source s
            ON t.id = s.id
            WHEN MATCHED AND s.action = 'U' THEN UPDATE SET
                name = s.name, score = s.score, status = 'first-matched'
            WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED AND s.action = 'I' THEN INSERT (id, name, score, status)
                VALUES (s.id, s.name, s.score, 'first-not-matched')
            WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                VALUES (s.id, s.name, s.score, 'fallback')
        """
        order_qt_paimon_merge_branch_priority """SELECT * FROM t_dml ORDER BY id"""

        sql """TRUNCATE TABLE internal.${dbName}.t_merge_source"""
        sql """INSERT INTO internal.${dbName}.t_merge_source VALUES
            (1, 'duplicate_1', 201, 'U'),
            (1, 'duplicate_2', 202, 'U')
        """
        test {
            sql """
                MERGE INTO t_dml t
                USING internal.${dbName}.t_merge_source s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score
            """
            exception "Paimon MERGE matched one target row with multiple source rows"
        }
        order_qt_paimon_merge_duplicate_unchanged """SELECT * FROM t_dml ORDER BY id"""

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
            exception "Paimon UPDATE only supports merge-engine=deduplicate"
        }
        test {
            sql """
                MERGE INTO t_append_only t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN DELETE
            """
            exception "Paimon MERGE requires a primary-key table"
        }

        sql """INSERT INTO t_ignore_delete VALUES (10, 'keep')"""
        test {
            sql """DELETE FROM t_ignore_delete WHERE id = 10"""
            exception "Paimon DELETE is not supported when ignore-delete=true"
        }
        test {
            sql """
                MERGE INTO t_ignore_delete t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN DELETE
            """
            exception "Paimon DELETE is not supported when ignore-delete=true"
        }
        order_qt_paimon_ignore_delete_unchanged """SELECT * FROM t_ignore_delete ORDER BY id"""

        sql """INSERT INTO t_partial_update VALUES (20, 'partial', 20)"""
        test {
            sql """UPDATE t_partial_update SET name = NULL WHERE id = 20"""
            exception "Paimon UPDATE only supports merge-engine=deduplicate"
        }
        test {
            sql """
                MERGE INTO t_partial_update t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET name = NULL
            """
            exception "Paimon UPDATE only supports merge-engine=deduplicate"
        }
        sql """DELETE FROM t_partial_update WHERE id = 20"""
        qt_paimon_partial_update_delete """SELECT count(*) FROM t_partial_update"""

        sql """INSERT INTO t_aggregation_no_delete VALUES (30, 30)"""
        test {
            sql """DELETE FROM t_aggregation_no_delete WHERE id = 30"""
            exception "Paimon DELETE does not support merge-engine=aggregation"
        }
        order_qt_paimon_aggregation_no_delete_unchanged """
            SELECT * FROM t_aggregation_no_delete ORDER BY id
        """

        sql """INSERT INTO t_aggregation_delete VALUES (40, 40)"""
        sql """DELETE FROM t_aggregation_delete WHERE id = 40"""
        qt_paimon_aggregation_delete """SELECT count(*) FROM t_aggregation_delete"""

        test {
            sql """
                MERGE INTO t_first_row t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET name = s.name
            """
            exception "Paimon UPDATE only supports merge-engine=deduplicate"
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
        sql """drop table if exists internal.${dbName}.t_merge_source"""
    }
}
