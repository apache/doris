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

// Doris pads a CHAR(n) value written into a Paimon table with spaces to n characters, as Spark and Flink do, so
// that a key written by Doris and by Spark is one row of a primary-key table. Doris reads a value back as it is
// stored, trailing spaces included, which the queries below show between brackets.
suite("test_paimon_write_char_padding", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_char_padding_catalog"
    String dbName = "test_pw_char_padding_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_pk;
        CREATE TABLE paimon.${dbName}.t_char_pk (
            c CHAR(3), v INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'c',
            'bucket' = '1'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_nested;
        CREATE TABLE paimon.${dbName}.t_char_nested (
            id INT, arr ARRAY<CHAR(3)>, m MAP<CHAR(2), CHAR(3)>, s STRUCT<code: CHAR(3), n: INT>
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_part;
        CREATE TABLE paimon.${dbName}.t_char_part (
            id INT, p CHAR(3)
        ) USING paimon
        PARTITIONED BY (p);

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_bucketed;
        CREATE TABLE paimon.${dbName}.t_char_bucketed (
            c CHAR(3), v INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'c',
            'bucket' = '4',
            'changelog-producer' = 'full-compaction'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_rowkind;
        CREATE TABLE paimon.${dbName}.t_char_rowkind (
            id INT, rk CHAR(3), code CHAR(3)
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'rowkind.field' = 'rk'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_rowkind_part;
        CREATE TABLE paimon.${dbName}.t_char_rowkind_part (
            id INT, rk CHAR(3)
        ) USING paimon
        PARTITIONED BY (rk)
        TBLPROPERTIES (
            'primary-key' = 'id,rk',
            'bucket' = '1',
            'rowkind.field' = 'rk'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_merge;
        CREATE TABLE paimon.${dbName}.t_char_merge (
            c CHAR(3), v INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'c',
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
            's3.path.style.access' = 'true'
        );
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    sql """create database if not exists internal.${dbName}"""
    sql """drop table if exists internal.${dbName}.char_merge_source"""
    sql """
        create table internal.${dbName}.char_merge_source (
            c char(3), v int
        ) duplicate key(c) distributed by hash(c) buckets 1
        properties ('replication_num' = '1')
    """

    try {
        // A primary key Doris writes is padded, and Spark's write of the same key replaces it.
        sql """INSERT INTO t_char_pk VALUES ('k', 1)"""
        order_qt_pk_padded """SELECT concat('[', c, ']'), char_length(c), v FROM t_char_pk"""
        spark_paimon """INSERT INTO paimon.${dbName}.t_char_pk VALUES ('k', 2)"""
        sql """refresh table t_char_pk"""
        order_qt_pk_one_row_with_spark """SELECT concat('[', c, ']'), char_length(c), v FROM t_char_pk"""

        // CHAR values nested in ARRAY, MAP and STRUCT are padded too; a NULL element stays NULL.
        sql """INSERT INTO t_char_nested VALUES
            (1, ['a', NULL, 'abc'], map('k', 'v'), named_struct('code', 'a', 'n', 7))"""
        order_qt_nested_padded """
            SELECT id, concat('[', arr[1], ']'), arr[2] IS NULL, concat('[', arr[3], ']'),
                concat('[', map_keys(m)[1], ']'), concat('[', map_values(m)[1], ']'),
                concat('[', struct_element(s, 'code'), ']'), struct_element(s, 'n')
            FROM t_char_nested
        """

        // A partition value is padded whether it comes from the rows or from a static PARTITION clause, so a
        // second static overwrite of 'x' replaces the partition the first one wrote.
        sql """INSERT OVERWRITE TABLE t_char_part PARTITION (p = 'x') SELECT 1"""
        sql """INSERT INTO t_char_part VALUES (2, 'y')"""
        order_qt_partition_padded """SELECT id, concat('[', p, ']') FROM t_char_part"""
        sql """INSERT OVERWRITE TABLE t_char_part PARTITION (p = 'x') SELECT 3"""
        order_qt_static_overwrite_replaces_padded """SELECT id, concat('[', p, ']') FROM t_char_part"""

        // With padding off, values are written as they are.
        sql """set enable_paimon_write_char_padding = false"""
        sql """INSERT INTO t_char_pk VALUES ('u', 3), ('w', 4)"""
        sql """INSERT OVERWRITE TABLE t_char_part PARTITION (p = 'z') SELECT 5"""
        sql """set enable_paimon_write_char_padding = true"""
        order_qt_pk_padding_off """SELECT concat('[', c, ']'), char_length(c), v FROM t_char_pk"""
        order_qt_partition_padding_off """SELECT id, concat('[', p, ']') FROM t_char_part"""

        // UPDATE and DELETE carry the key read back from the table, so with padding on they still match the
        // keys written unpadded: 'u' is deleted and 'w' is updated in place, without a padded duplicate.
        sql """DELETE FROM t_char_pk WHERE v = 3"""
        sql """UPDATE t_char_pk SET v = 40 WHERE v = 4"""
        order_qt_dml_matches_unpadded_keys """SELECT concat('[', c, ']'), char_length(c), v FROM t_char_pk"""

        // BE cannot route a CHAR bucket key by Paimon's bucket while padding is on, since it would hash the
        // unpadded value. Rows of one bucket would then reach several of the four writers, each would compact
        // the bucket, and Paimon would give up committing the compaction, and with it the changelog, after the
        // rows were committed. The write goes to one writer instead, and both snapshots are committed.
        sql """set parallel_pipeline_task_num = 4"""
        sql """INSERT INTO t_char_bucketed
            SELECT cast(number AS STRING), cast(number AS INT) FROM numbers("number" = "100")"""
        sql """set parallel_pipeline_task_num = 0"""
        order_qt_bucketed_padded """
            SELECT count(*), count(DISTINCT c), min(char_length(c)), max(char_length(c)) FROM t_char_bucketed
        """
        order_qt_bucketed_compaction_committed """
            SELECT snapshot_id, commit_kind, changelog_record_count FROM t_char_bucketed\$snapshots
        """

        // Paimon takes the row kind from the value of the rowkind.field as it is, so that value is not padded:
        // '-D' deletes the row of key 1.
        sql """INSERT INTO t_char_rowkind VALUES (1, '+I', 'a'), (2, '+I', 'b')"""
        sql """INSERT INTO t_char_rowkind VALUES (1, '-D', 'a')"""
        order_qt_rowkind_field_not_padded """
            SELECT id, concat('[', rk, ']'), concat('[', code, ']') FROM t_char_rowkind
        """

        // MERGE matches its source rows to the table, and checks the keys it inserts for duplicates, by the key as
        // Doris holds it, unpadded. Padding a shorter key that MERGE inserts would change the key after those
        // checks, so the MERGE fails and writes nothing. A MERGE that pads the key itself matches the padded
        // row, and its duplicate check then sees 'm' and 'm ' as one key.
        sql """INSERT INTO internal.${dbName}.char_merge_source VALUES ('k', 1)"""
        test {
            sql """
                MERGE INTO t_char_merge t
                USING internal.${dbName}.char_merge_source s ON t.c = s.c
                WHEN NOT MATCHED THEN INSERT (c, v) VALUES (s.c, s.v)
            """
            exception "rpad(<key>, 3, ' ')"
        }
        order_qt_merge_short_key_fails """SELECT concat('[', c, ']'), v FROM t_char_merge"""
        for (int i = 0; i < 2; i++) {
            // The first MERGE inserts 'k  ', the second one matches it.
            sql """
                MERGE INTO t_char_merge t
                USING internal.${dbName}.char_merge_source s ON t.c = rpad(s.c, 3, ' ')
                WHEN MATCHED THEN UPDATE SET v = t.v + 10
                WHEN NOT MATCHED THEN INSERT (c, v) VALUES (rpad(s.c, 3, ' '), s.v)
            """
        }
        order_qt_merge_padded_key """SELECT concat('[', c, ']'), char_length(c), v FROM t_char_merge"""
        sql """TRUNCATE TABLE internal.${dbName}.char_merge_source"""
        sql """INSERT INTO internal.${dbName}.char_merge_source VALUES ('m', 1), ('m ', 2)"""
        test {
            sql """
                MERGE INTO t_char_merge t
                USING internal.${dbName}.char_merge_source s ON t.c = rpad(s.c, 3, ' ')
                WHEN NOT MATCHED THEN INSERT (c, v) VALUES (rpad(s.c, 3, ' '), s.v)
            """
            exception "Connector MERGE attempted to insert multiple rows with the same primary key"
        }

        // With padding off, MERGE writes the keys as they are, as before: 'm' and 'm ' are two rows.
        sql """set enable_paimon_write_char_padding = false"""
        sql """
            MERGE INTO t_char_merge t
            USING internal.${dbName}.char_merge_source s ON t.c = s.c
            WHEN NOT MATCHED THEN INSERT (c, v) VALUES (s.c, s.v)
        """
        sql """set enable_paimon_write_char_padding = true"""
        order_qt_merge_padding_off """SELECT concat('[', c, ']'), char_length(c), v FROM t_char_merge"""

        // The static partition of an overwrite names a rowkind.field partition value as the rows carry it,
        // unpadded: an overwrite of '+I' replaces that partition, and one whose source turns out empty at run
        // time clears it.
        sql """INSERT INTO t_char_rowkind_part VALUES (1, '+I'), (2, '+I')"""
        sql """INSERT OVERWRITE TABLE t_char_rowkind_part PARTITION (rk = '+I') SELECT 3"""
        order_qt_rowkind_partition_overwrite """SELECT id, concat('[', rk, ']') FROM t_char_rowkind_part"""
        sql """
            INSERT OVERWRITE TABLE t_char_rowkind_part PARTITION (rk = '+I')
            SELECT v FROM internal.${dbName}.char_merge_source WHERE v < 0
        """
        order_qt_rowkind_partition_empty_overwrite """SELECT id, concat('[', rk, ']') FROM t_char_rowkind_part"""
    } finally {
        sql """set enable_paimon_write_char_padding = true"""
        sql """set parallel_pipeline_task_num = 0"""
    }
}
