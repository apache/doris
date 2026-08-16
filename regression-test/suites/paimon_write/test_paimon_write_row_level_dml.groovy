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

        DROP TABLE IF EXISTS paimon.${dbName}.t_partial_update_sequence_group;
        CREATE TABLE paimon.${dbName}.t_partial_update_sequence_group (
            id INT, name STRING, seq INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'partial-update',
            'fields.seq.sequence-group' = 'name',
            'partial-update.remove-record-on-sequence-group' = 'seq'
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

        DROP TABLE IF EXISTS paimon.${dbName}.t_sequence_ascending;
        CREATE TABLE paimon.${dbName}.t_sequence_ascending (
            id INT, seq1 INT, seq2 INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'sequence.field' = 'seq1,seq2',
            'sequence.field.sort-order' = 'ascending'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_sequence_descending;
        CREATE TABLE paimon.${dbName}.t_sequence_descending (
            id INT, seq INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'sequence.field' = 'seq',
            'sequence.field.sort-order' = 'descending'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_rowkind_field;
        CREATE TABLE paimon.${dbName}.t_rowkind_field (
            id INT, row_kind STRING, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'rowkind.field' = 'row_kind'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_input_changelog;
        CREATE TABLE paimon.${dbName}.t_input_changelog (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'changelog-producer' = 'input'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_cross_partition_fixed;
        CREATE TABLE paimon.${dbName}.t_cross_partition_fixed (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_cross_partition_ttl;
        CREATE TABLE paimon.${dbName}.t_cross_partition_ttl (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'cross-partition-upsert.index-ttl' = '1 h'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_cross_partition_ignore_delete;
        CREATE TABLE paimon.${dbName}.t_cross_partition_ignore_delete (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'ignore-delete' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_same_name;
        CREATE TABLE paimon.${dbName}.t_same_name (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_binary;
        CREATE TABLE paimon.${dbName}.t_binary (
            id INT, payload BINARY
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1'
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
            's3.path.style.access' = 'true',
            'enable.mapping.varbinary' = 'true'
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
    sql """drop table if exists internal.${dbName}.t_delete_source"""
    sql """
        CREATE TABLE internal.${dbName}.t_delete_source (
            id INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """
    sql """
        INSERT INTO internal.${dbName}.t_delete_source
        SELECT 10 FROM numbers("number" = "1024")
    """
    sql """drop table if exists internal.${dbName}.t_same_name"""
    sql """
        CREATE TABLE internal.${dbName}.t_same_name (
            id INT, name STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    try {
        def latestSnapshotId = { String tableName ->
            def rows = spark_paimon """
                SELECT max(snapshot_id)
                FROM paimon.${dbName}.`${tableName}\$snapshots`
            """
            assertEquals(1, rows.size())
            assertTrue(rows[0][0] != null)
            return rows[0][0].toString()
        }

        def incrementalAuditLog = { tableName, columns, beforeSnapshot, afterSnapshot ->
            spark_paimon """
                SELECT ${columns}
                FROM paimon_incremental_query(
                    'paimon.${dbName}.`${tableName}\$audit_log`',
                    '${beforeSnapshot}',
                    '${afterSnapshot}'
                )
                ORDER BY id, rowkind
            """
        }

        sql """INSERT INTO t_dml VALUES
            (1, 'Alice', 10, 'active'),
            (2, 'Bob', 20, 'active'),
            (3, 'Charlie', 30, 'active'),
            (4, 'Diana', 40, 'active')
        """
        sql """INSERT INTO t_sequence_ascending VALUES (1, 10, 20, 'ascending')"""
        sql """INSERT INTO t_sequence_descending VALUES (1, 10, 'descending')"""

        test {
            sql """UPDATE t_sequence_ascending SET seq2 = 30 WHERE id = 1"""
            exception "Paimon UPDATE cannot modify sequence-field column 'seq2'"
        }
        test {
            sql """
                MERGE INTO t_sequence_descending t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET seq = s.score, name = s.name
            """
            exception "Paimon UPDATE cannot modify sequence-field column 'seq'"
        }
        test {
            sql """UPDATE t_rowkind_field SET name = 'updated' WHERE id = 1"""
            exception "Paimon UPDATE is not supported when rowkind.field is configured"
        }
        test {
            sql """DELETE FROM t_rowkind_field WHERE id = 1"""
            exception "Paimon DELETE is not supported when rowkind.field is configured"
        }
        test {
            sql """
                MERGE INTO t_rowkind_field t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN DELETE
            """
            exception "Paimon MERGE is not supported when rowkind.field is configured"
        }
        test {
            sql """UPDATE t_input_changelog SET name = 'updated' WHERE id = 1"""
            exception "Paimon UPDATE is not supported when changelog-producer=input"
        }
        test {
            sql """
                MERGE INTO t_input_changelog t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET name = s.name
            """
            exception "Paimon UPDATE is not supported when changelog-producer=input"
        }
        test {
            sql """UPDATE t_cross_partition_fixed SET pt = 'new_pt' WHERE id = 1"""
            exception "Paimon UPDATE cannot modify partition column 'pt' unless bucket=-1"
        }
        test {
            sql """
                MERGE INTO t_cross_partition_fixed t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET pt = 'new_pt', name = s.name
            """
            exception "Paimon UPDATE cannot modify partition column 'pt' unless bucket=-1"
        }
        test {
            sql """UPDATE t_cross_partition_ttl SET name = 'updated' WHERE id = 1"""
            exception "cross-partition-upsert.index-ttl is configured"
        }
        test {
            sql """DELETE FROM t_cross_partition_ttl WHERE id = 1"""
            exception "cross-partition-upsert.index-ttl is configured"
        }
        test {
            sql """
                MERGE INTO t_cross_partition_ttl t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN DELETE
            """
            exception "cross-partition-upsert.index-ttl is configured"
        }
        test {
            sql """
                UPDATE t_cross_partition_ignore_delete
                SET pt = 'new_pt' WHERE id = 1
            """
            exception "cannot modify partition column 'pt' when ignore-delete=true"
        }
        test {
            sql """
                MERGE INTO t_cross_partition_ignore_delete t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET pt = 'new_pt', name = s.name
            """
            exception "cannot modify partition column 'pt' when ignore-delete=true"
        }

        sql """INSERT INTO t_input_changelog VALUES (10, 'delete-me')"""
        String deleteInputBefore = latestSnapshotId("t_input_changelog")
        sql """
            DELETE FROM t_input_changelog t
            USING internal.${dbName}.t_delete_source s
            WHERE t.id = s.id
        """
        String deleteInputAfter = latestSnapshotId("t_input_changelog")
        assertEquals([
                ["-D", 10, "delete-me"]
        ], incrementalAuditLog("t_input_changelog", "rowkind, id, name",
                deleteInputBefore, deleteInputAfter))
        assertEquals([[0L]], sql("SELECT count(*) FROM t_input_changelog"))

        sql """INSERT INTO t_binary VALUES (1, CAST('delete-me' AS VARBINARY))"""
        sql """DELETE FROM t_binary WHERE id = 1"""
        assertEquals([[0L]], sql("SELECT count(*) FROM t_binary"))

        sql """INSERT INTO t_same_name VALUES (1, 'old')"""
        sql """INSERT INTO internal.${dbName}.t_same_name VALUES (1, 'from-update')"""
        sql """
            UPDATE ${catalogName}.${dbName}.t_same_name
            SET name = internal.${dbName}.t_same_name.name
            FROM internal.${dbName}.t_same_name
            WHERE ${catalogName}.${dbName}.t_same_name.id
                    = internal.${dbName}.t_same_name.id
        """
        assertEquals([[1, "from-update"]], sql("SELECT * FROM t_same_name"))
        sql """TRUNCATE TABLE internal.${dbName}.t_same_name"""
        sql """INSERT INTO internal.${dbName}.t_same_name VALUES (1, 'from-merge')"""
        sql """
            MERGE INTO t_same_name
            USING internal.${dbName}.t_same_name
            ON ${catalogName}.${dbName}.t_same_name.id
                    = internal.${dbName}.t_same_name.id
            WHEN MATCHED THEN UPDATE
                SET name = internal.${dbName}.t_same_name.name
        """
        assertEquals([[1, "from-merge"]], sql("SELECT * FROM t_same_name"))
        sql """
            DELETE FROM ${catalogName}.${dbName}.t_same_name
            USING internal.${dbName}.t_same_name
            WHERE ${catalogName}.${dbName}.t_same_name.id
                    = internal.${dbName}.t_same_name.id
        """
        assertEquals([[0L]], sql("SELECT count(*) FROM t_same_name"))

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
            sql """
                MERGE INTO t_dml t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                    VALUES (s.id + 1, s.name, s.score, 'invalid-insert-key')
            """
            exception "each INSERT to use the corresponding deterministic source expression"
        }
        test {
            sql """
                MERGE INTO t_dml t
                USING internal.${dbName}.t_merge_source s
                ON t.id = s.id AND t.status = 'not-present'
                WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                    VALUES (s.id, s.name, s.score, 'invalid-extra-predicate')
            """
            exception "Paimon MERGE with NOT MATCHED INSERT requires ON to contain only equality predicates"
        }

        sql """TRUNCATE TABLE internal.${dbName}.t_merge_source"""
        sql """INSERT INTO internal.${dbName}.t_merge_source VALUES
            (1000, 'duplicate_insert_1', 301, 'I'),
            (1000, 'duplicate_insert_2', 302, 'I')
        """
        test {
            sql """
                MERGE INTO t_dml t
                USING internal.${dbName}.t_merge_source s
                ON t.id = s.id
                WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                    VALUES (s.id, s.name, s.score, 'duplicate-insert')
            """
            exception "Paimon MERGE attempted to insert multiple rows with the same primary key"
        }
        order_qt_paimon_merge_duplicate_insert_unchanged """SELECT * FROM t_dml ORDER BY id"""

        test {
            sql """
                MERGE INTO t_dml t
                USING internal.${dbName}.t_merge_source s
                ON t.id = CAST(RAND() * 1000000 AS INT)
                WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                    VALUES (CAST(RAND() * 1000000 AS INT), s.name, s.score, 'random-key')
            """
            exception "each INSERT to use the corresponding deterministic source expression"
        }

        sql """TRUNCATE TABLE internal.${dbName}.t_merge_source"""
        sql """
            INSERT INTO internal.${dbName}.t_merge_source
            SELECT CAST(number + 2000 AS INT), CONCAT('unmatched_', number),
                   CAST(number AS INT), 'I'
            FROM numbers("number" = "1024")
        """
        sql """
            MERGE INTO t_dml t
            USING internal.${dbName}.t_merge_source s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                VALUES (s.id, s.name, s.score, 'many-unmatched')
        """
        qt_paimon_merge_many_unmatched """SELECT count(*) FROM t_dml WHERE id >= 2000"""

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

        sql """INSERT INTO t_partial_update_sequence_group VALUES (21, 'keep', NULL)"""
        test {
            sql """DELETE FROM t_partial_update_sequence_group WHERE id = 21"""
            exception "partial-update.remove-record-on-delete=true"
        }
        sql """TRUNCATE TABLE internal.${dbName}.t_merge_source"""
        sql """INSERT INTO internal.${dbName}.t_merge_source VALUES
            (21, 'keep', 0, 'D')
        """
        test {
            sql """
                MERGE INTO t_partial_update_sequence_group t
                USING internal.${dbName}.t_merge_source s ON t.id = s.id
                WHEN MATCHED THEN DELETE
            """
            exception "partial-update.remove-record-on-delete=true"
        }

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

        sql """set short_circuit_evaluation = false"""
        sql """TRUNCATE TABLE internal.${dbName}.t_merge_source"""
        sql """INSERT INTO internal.${dbName}.t_merge_source VALUES
            (1, 'short_update', 901, 'U'),
            (7000, 'short_insert', 902, 'I')
        """
        sql """
            MERGE INTO t_dml t
            USING internal.${dbName}.t_merge_source s ON t.id = s.id
            WHEN MATCHED AND s.id <=> 1 THEN UPDATE SET
                name = s.name,
                score = IF(assert_true(s.id = 1, 'inactive assignment evaluated'), s.score, -1)
            WHEN MATCHED AND assert_true(s.id < 0, 'later predicate evaluated') THEN DELETE
            WHEN NOT MATCHED THEN INSERT (id, name, score, status)
                VALUES (s.id, s.name, s.score, 'short-circuit')
        """
        assertEquals([["short_update"], ["short_insert"]],
                sql("SELECT name FROM t_dml WHERE id IN (1, 7000) ORDER BY id"))
    } finally {
        sql """drop catalog if exists ${catalogName}"""
        sql """drop table if exists internal.${dbName}.t_merge_source"""
        sql """drop table if exists internal.${dbName}.t_delete_source"""
        sql """drop table if exists internal.${dbName}.t_same_name"""
    }
}
