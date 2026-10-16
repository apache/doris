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

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_orc_bucketed;
        CREATE TABLE paimon.${dbName}.t_char_orc_bucketed (
            c CHAR(3), v INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'c',
            'bucket' = '4',
            'file.format' = 'orc'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_rowkind_key;
        CREATE TABLE paimon.${dbName}.t_char_rowkind_key (
            c CHAR(3), rk STRING, v INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'c',
            'bucket' = '1',
            'rowkind.field' = 'rk'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_map_avro;
        CREATE TABLE paimon.${dbName}.t_char_map_avro (
            id INT, m MAP<CHAR(2), INT>
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'file.format' = 'avro'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_rowkind_default;
        CREATE TABLE paimon.${dbName}.t_char_rowkind_default (
            c CHAR(3), rk STRING DEFAULT '+I', v INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'c',
            'bucket' = '1',
            'rowkind.field' = 'rk'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_rowkind_overwrite;
        CREATE TABLE paimon.${dbName}.t_char_rowkind_overwrite (
            id INT, p CHAR(3), rk STRING, v INT
        ) USING paimon
        PARTITIONED BY (p)
        TBLPROPERTIES (
            'primary-key' = 'id,p',
            'bucket' = '1',
            'rowkind.field' = 'rk'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_map_row_key;
        CREATE TABLE paimon.${dbName}.t_char_map_row_key (
            id INT, m MAP<STRUCT<c: CHAR(2), r: STRUCT<i: INT>>, INT>
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_map_nested_map_key;
        CREATE TABLE paimon.${dbName}.t_char_map_nested_map_key (
            id INT, m MAP<STRUCT<c: CHAR(2), m: MAP<STRING, INT>>, INT>
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'file.format' = 'avro'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_map_nan_key;
        CREATE TABLE paimon.${dbName}.t_char_map_nan_key (
            id INT, m MAP<STRUCT<c: CHAR(2), d: DOUBLE>, INT>
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'file.format' = 'avro'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_char_map_nested_nan_key;
        CREATE TABLE paimon.${dbName}.t_char_map_nested_nan_key (
            id INT, m MAP<STRUCT<c: CHAR(2), m: MAP<DOUBLE, INT>>, INT>
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'file.format' = 'avro'
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
    sql """drop table if exists internal.${dbName}.char_nan_source"""
    sql """
        create table internal.${dbName}.char_nan_source (
            pos varchar(8), neg varchar(8)
        ) duplicate key(pos) distributed by hash(pos) buckets 1
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

        // Padding would turn the distinct map keys 'k' and 'k ' into the same key 'k ', and a map holds one entry
        // per key (Paimon's Avro reader keeps one of the two), so such a map fails the write. Keys that stay
        // distinct are padded, and with padding off both keys are written as they are.
        test {
            sql """INSERT INTO t_char_map_avro VALUES (1, map('k', 1, 'k ', 2))"""
            exception "turns the keys 'k' and 'k ' into the same key 'k '"
        }
        sql """INSERT INTO t_char_map_avro VALUES (2, map('k', 1, 'kk', 2))"""
        sql """set enable_paimon_write_char_padding = false"""
        sql """INSERT INTO t_char_map_avro VALUES (3, map('k', 1, 'k ', 2))"""
        sql """set enable_paimon_write_char_padding = true"""
        order_qt_map_keys_made_equal_fail """
            SELECT id, map_size(m), concat('[', array_join(array_sort(map_keys(m)), '|'), ']'),
                array_join(array_sort(map_values(m)), '|')
            FROM t_char_map_avro
        """

        // The writer takes a map key from the Arrow batch BE sends, where a ROW key holding a ROW is a columnar row
        // that cannot be hashed, so keys are compared by their bytes: such a key is padded, and two keys that
        // padding makes equal fail the row as above.
        sql """INSERT INTO t_char_map_row_key VALUES (1, map(named_struct('c', 'k', 'r', named_struct('i', 1)), 7))"""
        test {
            sql """
                INSERT INTO t_char_map_row_key VALUES (2, map(named_struct('c', 'k', 'r', named_struct('i', 1)), 1,
                    named_struct('c', 'k ', 'r', named_struct('i', 1)), 2))
            """
            exception "into the same key"
        }
        order_qt_map_row_key_padded """
            SELECT id, map_size(m), concat('[', struct_element(map_keys(m)[1], 'c'), ']'), map_values(m)[1]
            FROM t_char_map_row_key
        """

        // Paimon compares maps regardless of the order of their entries, and its Avro reader keeps one of two keys
        // whose nested maps differ only in that order, so such keys that padding makes equal fail the row too. A
        // key holding a map is padded.
        test {
            sql """
                INSERT INTO t_char_map_nested_map_key VALUES (1, map(named_struct('c', 'k', 'm', map('a', 1, 'b', 2)),
                    10, named_struct('c', 'k ', 'm', map('b', 2, 'a', 1)), 20))
            """
            exception "into the same key"
        }
        sql """INSERT INTO t_char_map_nested_map_key VALUES (2, map(named_struct('c', 'k', 'm', map('a', 1)), 10))"""
        order_qt_map_nested_map_key_padded """
            SELECT id, map_size(m), concat('[', struct_element(map_keys(m)[1], 'c'), ']'), map_values(m)[1]
            FROM t_char_map_nested_map_key
        """

        // Paimon compares a DOUBLE regardless of the bits of a NaN, and its Avro reader keeps one of two keys that
        // differ only so, while a NaN cast at run time keeps its sign. So the keys (k, NaN) and (k , -NaN), which
        // padding makes equal, fail the row too. A key holding a NaN is padded.
        sql """INSERT INTO internal.${dbName}.char_nan_source VALUES ('NaN', '-NaN')"""
        test {
            sql """
                INSERT INTO t_char_map_nan_key
                SELECT 1, map(named_struct('c', 'k', 'd', cast(pos AS DOUBLE)), 10,
                    named_struct('c', 'k ', 'd', cast(neg AS DOUBLE)), 20)
                FROM internal.${dbName}.char_nan_source
            """
            exception "into the same key"
        }
        sql """
            INSERT INTO t_char_map_nan_key
            SELECT 2, map(named_struct('c', 'k', 'd', cast(neg AS DOUBLE)), 10) FROM internal.${dbName}.char_nan_source
        """
        order_qt_map_nan_key_padded """
            SELECT id, map_size(m), concat('[', struct_element(map_keys(m)[1], 'c'), ']'),
                struct_element(map_keys(m)[1], 'd'), map_values(m)[1]
            FROM t_char_map_nan_key
        """

        // Paimon's Avro reader keeps the last of the entries of a map whose keys are equal, so a nested map holding
        // NaN -> 1 and -NaN -> 2 reads back as {NaN: 2}, and the keys (k, {NaN: 1, -NaN: 2}) and (k , {NaN: 2}),
        // which padding then makes equal, fail the row too. Keys whose nested maps keep other last values are padded.
        test {
            sql """
                INSERT INTO t_char_map_nested_nan_key
                SELECT 1, map(named_struct('c', 'k', 'm', map(cast(pos AS DOUBLE), 1, cast(neg AS DOUBLE), 2)), 10,
                    named_struct('c', 'k ', 'm', map(cast(pos AS DOUBLE), 2)), 20)
                FROM internal.${dbName}.char_nan_source
            """
            exception "into the same key"
        }
        sql """
            INSERT INTO t_char_map_nested_nan_key
            SELECT 2, map(named_struct('c', 'k', 'm', map(cast(pos AS DOUBLE), 1, cast(neg AS DOUBLE), 2)), 10,
                named_struct('c', 'k ', 'm', map(cast(pos AS DOUBLE), 1)), 20)
            FROM internal.${dbName}.char_nan_source
        """
        order_qt_map_nested_nan_key_padded """
            SELECT id, map_size(m),
                array_join(array_sort(array_map(k -> concat('[', struct_element(k, 'c'), ']'), map_keys(m))), '|'),
                array_join(array_sort(map_values(m)), '|')
            FROM t_char_map_nested_nan_key
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

        // ORC pads every CHAR value it writes and strips the trailing spaces from one it reads, so only the
        // partition values of an ORC table are padded: padding a key would only change its bucket, and the DELETE
        // and UPDATE below, which read the keys back without the spaces, would hash other buckets and miss them.
        sql """INSERT INTO t_char_orc_bucketed VALUES ('k', 1), ('ab', 2), ('x', 3), ('m', 4), ('zz', 5), ('abc', 6)"""
        sql """DELETE FROM t_char_orc_bucketed WHERE v IN (1, 3, 4, 5)"""
        sql """UPDATE t_char_orc_bucketed SET v = v * 10 WHERE v IN (2, 6)"""
        order_qt_orc_dml_reaches_keys """SELECT concat('[', c, ']'), char_length(c), v FROM t_char_orc_bucketed"""

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

        // Paimon takes the kind of a row from its rowkind.field, so only a '+I' row inserts and is padded. A '-D'
        // row carries a key to delete, which Doris reads back as it is stored: here 'k', written with padding off,
        // and 'k  '.
        sql """set enable_paimon_write_char_padding = false"""
        sql """INSERT INTO t_char_rowkind_key VALUES ('k', '+I', 1)"""
        sql """set enable_paimon_write_char_padding = true"""
        sql """INSERT INTO t_char_rowkind_key VALUES ('k', '+I', 2), ('j', '+I', 3)"""
        order_qt_rowkind_key_inserted """SELECT concat('[', c, ']'), rk, v FROM t_char_rowkind_key"""
        sql """INSERT INTO t_char_rowkind_key SELECT c, '-D', v FROM t_char_rowkind_key WHERE v IN (1, 2)"""
        order_qt_rowkind_key_deleted """SELECT concat('[', c, ']'), rk, v FROM t_char_rowkind_key"""

        // Paimon decodes the row kind ignoring case, after filling a NULL with the field's default, so '+i' and a
        // NULL under DEFAULT '+I' insert and are padded: the '+I' rows of the same keys replace them.
        sql """INSERT INTO t_char_rowkind_default VALUES ('k', '+i', 1), ('j', NULL, 2)"""
        sql """INSERT INTO t_char_rowkind_default VALUES ('k', '+I', 3), ('j', '+I', 4)"""
        order_qt_rowkind_default_and_case """SELECT concat('[', c, ']'), rk, v FROM t_char_rowkind_default"""

        // An overwrite replaces the partitions it writes, so none of its rows matches a stored key, and all of them
        // are padded like the static partition value: a '+U' row lands in the overwritten partition 'x  ', and a
        // '+I' and a '-D' of one key cancel out, leaving partition 'y  ' empty.
        sql """INSERT OVERWRITE TABLE t_char_rowkind_overwrite PARTITION (p = 'x') SELECT 1, '+U', 9"""
        sql """INSERT OVERWRITE TABLE t_char_rowkind_overwrite PARTITION (p = 'y') VALUES (2, '+I', 1), (2, '-D', 1)"""
        order_qt_rowkind_overwrite_padded """SELECT id, concat('[', p, ']'), rk, v FROM t_char_rowkind_overwrite"""

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
