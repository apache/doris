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

suite("test_paimon_schema_time_travel_matrix", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_schema_time_travel_matrix"
    String noCacheCatalogName = "test_paimon_schema_time_travel_matrix_no_cache"
    String dbName = "paimon_schema_time_travel_matrix_db"
    String topTable = "top_timeline"
    String nestedTable = "nested_timeline"
    String pkTable = "pk_dv_timeline"
    String partitionTable = "partition_timeline"

    def latestSnapshotId = { String tableName ->
        List<List<Object>> rows = spark_paimon """
            SELECT snapshot_id
            FROM paimon.${dbName}.`${tableName}\$snapshots`
            ORDER BY snapshot_id DESC
            LIMIT 1
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    def createTag = { String tableName, String tagName, String snapshotId ->
        spark_paimon """
            CALL paimon.sys.create_tag(
                table => '${dbName}.${tableName}',
                tag => '${tagName}',
                snapshot => ${snapshotId}
            )
        """
    }

    def assertUnknownColumn = { String query, String columnName ->
        test {
            sql query
            exception "'${columnName}'"
        }
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """drop catalog if exists ${noCacheCatalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true'
        )
    """
    sql """
        CREATE CATALOG ${noCacheCatalogName} PROPERTIES (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        spark_paimon_multi """
            CREATE DATABASE IF NOT EXISTS paimon.${dbName};
            DROP TABLE IF EXISTS paimon.${dbName}.${topTable};
            CREATE TABLE paimon.${dbName}.${topTable} (
                id INT,
                old_name STRING,
                victim STRING,
                metric INT
            ) USING paimon
            TBLPROPERTIES ('file.format'='parquet');
            INSERT INTO paimon.${dbName}.${topTable}
                VALUES (1, 'alpha', 'old-v1', 10);
        """
        String topCp0 = latestSnapshotId(topTable)
        createTag(topTable, "top_cp0", topCp0)
        spark_paimon """
            CALL paimon.sys.create_branch(
                '${dbName}.${topTable}',
                'top_cp0_branch',
                'top_cp0'
            )
        """
        Thread.sleep(1100)

        // Scenario S01/S02/S03 x T00/T01/T02/T05/T08:
        // add multiple columns and position one AFTER old_name.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${topTable}
                ADD COLUMNS (added STRING AFTER old_name, added_second BIGINT);
            INSERT INTO paimon.${dbName}.${topTable}
                (id, old_name, victim, metric, added, added_second)
                VALUES (2, 'beta', 'old-v2', 20, 'added-v2', 200);
        """
        String topCpAdd = latestSnapshotId(topTable)
        createTag(topTable, "top_cp_add", topCpAdd)
        Thread.sleep(1100)

        // Scenario S04/S05 x T00-T08:
        // explicit old/new column binding catches rename regressions hidden by SELECT *.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${topTable}
                RENAME COLUMN old_name TO MixedName;
            INSERT INTO paimon.${dbName}.${topTable}
                (id, MixedName, victim, metric, added, added_second)
                VALUES (3, 'gamma', 'old-v3', 30, 'added-v3', 300);
        """
        String topCpRename = latestSnapshotId(topTable)
        createTag(topTable, "top_cp_rename", topCpRename)
        Thread.sleep(1100)

        // Scenario S06 x T00/T01/T02/T05: the dropped field remains available only to old refs.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${topTable} DROP COLUMN victim;
            INSERT INTO paimon.${dbName}.${topTable}
                (id, MixedName, metric, added, added_second)
                VALUES (4, 'delta', 40, 'added-v4', 400);
        """
        String topCpDrop = latestSnapshotId(topTable)
        createTag(topTable, "top_cp_drop", topCpDrop)
        Thread.sleep(1100)

        // Scenario S07 x T00/T01/T02/T05/T12/T13:
        // drop/re-add with a different type creates a new field ID and must not expose old values.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${topTable} ADD COLUMN victim BIGINT;
            INSERT INTO paimon.${dbName}.${topTable}
                (id, MixedName, metric, added, added_second, victim)
                VALUES (5, 'epsilon', 50, 'added-v5', 500, 5000);
        """
        String topCpReadd = latestSnapshotId(topTable)
        createTag(topTable, "top_cp_readd", topCpReadd)
        Thread.sleep(1100)

        // Scenario S08 x T00/T01/T02/T05: compatible INT -> BIGINT promotion.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${topTable} ALTER COLUMN metric TYPE BIGINT;
            INSERT INTO paimon.${dbName}.${topTable}
                (id, MixedName, metric, added, added_second, victim)
                VALUES (6, 'zeta', 6000000000, 'added-v6', 600, 6000);
        """
        String topCpPromote = latestSnapshotId(topTable)
        createTag(topTable, "top_cp_promote", topCpPromote)

        spark_paimon_multi """
            DROP TABLE IF EXISTS paimon.${dbName}.${nestedTable};
            CREATE TABLE paimon.${dbName}.${nestedTable} (
                id INT,
                payload STRUCT<old_child: INT, keep: INT>,
                attributes MAP<STRING, STRUCT<old_child: INT, keep: INT>>,
                events ARRAY<STRUCT<old_child: INT, keep: INT>>
            ) USING paimon
            TBLPROPERTIES ('file.format'='orc');
            INSERT INTO paimon.${dbName}.${nestedTable} VALUES (
                1,
                named_struct('old_child', 10, 'keep', 11),
                map('a', named_struct('old_child', 20, 'keep', 21)),
                array(named_struct('old_child', 30, 'keep', 31))
            );
        """
        String nestedCp0 = latestSnapshotId(nestedTable)
        createTag(nestedTable, "nested_cp0", nestedCp0)

        // Scenario S09/S14/S15 x T00/T01/T02/T05:
        // add children to STRUCT, MAP value struct and ARRAY element struct.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${nestedTable} ADD COLUMN payload.added_child INT;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                ADD COLUMN attributes.value.added_child INT;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                ADD COLUMN events.element.added_child INT;
            INSERT INTO paimon.${dbName}.${nestedTable} VALUES (
                2,
                named_struct('old_child', 110, 'keep', 111, 'added_child', 112),
                map('a', named_struct('old_child', 120, 'keep', 121, 'added_child', 122)),
                array(named_struct('old_child', 130, 'keep', 131, 'added_child', 132))
            );
        """
        String nestedCpAdd = latestSnapshotId(nestedTable)

        // Scenario S10/S14/S15 x T00/T01/T02/T05: rename every supported nested path.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${nestedTable}
                RENAME COLUMN payload.old_child TO renamed_child;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                RENAME COLUMN attributes.value.old_child TO renamed_child;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                RENAME COLUMN events.element.old_child TO renamed_child;
            INSERT INTO paimon.${dbName}.${nestedTable} VALUES (
                3,
                named_struct('renamed_child', 210, 'keep', 211, 'added_child', 212),
                map('a', named_struct('renamed_child', 220, 'keep', 221, 'added_child', 222)),
                array(named_struct('renamed_child', 230, 'keep', 231, 'added_child', 232))
            );
        """
        String nestedCpRename = latestSnapshotId(nestedTable)
        createTag(nestedTable, "nested_cp_rename", nestedCpRename)

        // Scenario S11/S12/S13 x T00/T01/T02/T05:
        // nested drop/re-add and type promotion preserve field-ID isolation.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${nestedTable} DROP COLUMN payload.renamed_child;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                DROP COLUMN attributes.value.renamed_child;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                DROP COLUMN events.element.renamed_child;
            ALTER TABLE paimon.${dbName}.${nestedTable} ADD COLUMN payload.renamed_child BIGINT;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                ADD COLUMN attributes.value.renamed_child BIGINT;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                ADD COLUMN events.element.renamed_child BIGINT;
            ALTER TABLE paimon.${dbName}.${nestedTable}
                ALTER COLUMN payload.keep TYPE BIGINT;
            INSERT INTO paimon.${dbName}.${nestedTable} VALUES (
                4,
                named_struct('keep', 311, 'added_child', 312, 'renamed_child', 3100),
                map('a', named_struct('keep', 321, 'added_child', 322, 'renamed_child', 3200)),
                array(named_struct('keep', 331, 'added_child', 332, 'renamed_child', 3300))
            );
        """
        String nestedCpReadd = latestSnapshotId(nestedTable)

        spark_paimon_multi """
            DROP TABLE IF EXISTS paimon.${dbName}.${pkTable};
            CREATE TABLE paimon.${dbName}.${pkTable} (
                id INT NOT NULL,
                old_name STRING,
                note STRING,
                score INT
            ) USING paimon
            TBLPROPERTIES (
                'bucket'='1',
                'primary-key'='id',
                'file.format'='parquet',
                'deletion-vectors.enabled'='true'
            );
            INSERT INTO paimon.${dbName}.${pkTable} VALUES
                (1, 'alpha', 'old-note-1', 10),
                (2, 'beta', 'old-note-2', 20);
        """
        String pkCp0 = latestSnapshotId(pkTable)
        createTag(pkTable, "pk_cp0", pkCp0)

        // Scenario S01/S04/S18 x TC05:
        // PK remains stable while a non-key field is added/renamed and rows are upserted/deleted.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${pkTable} ADD COLUMN extra STRING;
            ALTER TABLE paimon.${dbName}.${pkTable} RENAME COLUMN old_name TO full_name;
            INSERT INTO paimon.${dbName}.${pkTable}
                (id, full_name, note, score, extra)
                VALUES (1, 'alpha-updated', 'new-note-1', 11, 'extra-1');
            DELETE FROM paimon.${dbName}.${pkTable} WHERE id = 2;
        """
        String pkCpRenameDelete = latestSnapshotId(pkTable)
        createTag(pkTable, "pk_cp_rename_delete", pkCpRenameDelete)

        // Scenario S06/S07/S08/S18 x TC05:
        // drop/re-add and promotion are combined with a later upsert and DV compaction.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${pkTable} DROP COLUMN note;
            ALTER TABLE paimon.${dbName}.${pkTable} ADD COLUMN note BIGINT;
            ALTER TABLE paimon.${dbName}.${pkTable} ALTER COLUMN score TYPE BIGINT;
            INSERT INTO paimon.${dbName}.${pkTable}
                (id, full_name, score, extra, note)
                VALUES (3, 'gamma', 3000000000, 'extra-3', 3000);
        """
        String pkCpReadd = latestSnapshotId(pkTable)
        createTag(pkTable, "pk_cp_readd", pkCpReadd)
        spark_paimon """
            CALL paimon.sys.compact(table => '${dbName}.${pkTable}', compact_strategy => 'full')
        """

        spark_paimon_multi """
            DROP TABLE IF EXISTS paimon.${dbName}.${partitionTable};
            CREATE TABLE paimon.${dbName}.${partitionTable} (
                id INT,
                old_partition STRING,
                payload STRING
            ) USING paimon
            PARTITIONED BY (old_partition)
            TBLPROPERTIES ('file.format'='parquet');
            INSERT INTO paimon.${dbName}.${partitionTable}
                VALUES (1, 'p1', 'old'), (2, 'p2', 'old');
        """
        String partitionCp0 = latestSnapshotId(partitionTable)
        createTag(partitionTable, "partition_cp0", partitionCp0)

        // Scenario S16-negative: Paimon must reject partition-column rename atomically.
        String partitionRenameError = null
        try {
            spark_paimon """
                ALTER TABLE paimon.${dbName}.${partitionTable}
                RENAME COLUMN old_partition TO new_partition
            """
        } catch (Exception e) {
            partitionRenameError = e.getMessage()
        }
        assertNotNull(partitionRenameError)
        assertTrue(partitionRenameError.contains("Cannot rename partition column"))

        // Scenario S17 x TC03: rename a payload field while retaining partition pruning.
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.${partitionTable}
                RENAME COLUMN payload TO new_payload;
            INSERT INTO paimon.${dbName}.${partitionTable}
                VALUES (3, 'p3', 'new');
        """
        String partitionCpRename = latestSnapshotId(partitionTable)

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh catalog ${catalogName}"""

        // Scenario TC01: validate latest schema/data, explicit new binding, predicate and aggregate.
        assertEquals([[1, null], [2, null], [3, null], [4, null], [5, 5000L], [6, 6000L]],
                sql("""select id, victim from ${topTable} order by id"""))
        assertEquals([[6, 6000000000L]],
                sql("""select id, metric from ${topTable} where metric > 5000000000"""))
        assertEquals([[6L, 6000000150L]],
                sql("""select count(*), sum(metric) from ${topTable}"""))
        assertUnknownColumn("""select old_name from ${topTable}""", "old_name")

        // Scenario T01/T05/T06/T08: old snapshot/tag/branch uses the old schema.
        List<List<Object>> topCp0Rows = [[1, "alpha", "old-v1", 10]]
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of ${topCp0}
            order by id
        """))
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of 'top_cp0'
            order by id
        """))
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable}@tag(top_cp0)
            order by id
        """))
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable}@branch(top_cp0_branch)
            order by id
        """))
        assertUnknownColumn("""
            select MixedName from ${topTable} for version as of ${topCp0}
        """, "MixedName")

        // Scenario T03/T04: time string and epoch millis resolve to the pre-change schema.
        List<List<Object>> cp0TimeRows = sql("""
            select date_format(date_add(commit_time, interval 1 second), '%Y-%m-%d %H:%i:%s'),
                   cast(unix_timestamp(commit_time) * 1000 + 999 as bigint)
            from ${topTable}\$snapshots
            where snapshot_id = ${topCp0}
        """)
        assertEquals(1, cp0TimeRows.size())
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable} for time as of "${cp0TimeRows[0][0]}"
            order by id
        """))
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable} for time as of ${cp0TimeRows[0][1]}
            order by id
        """))

        // Scenario S01-S08: checkpoint projections prove name/type/field-ID isolation.
        assertEquals([[1, "alpha", null], [2, "beta", "added-v2"]],
                sql("""
                    select id, old_name, added
                    from ${topTable} for version as of ${topCpAdd}
                    order by id
                """))
        assertEquals([[1, "alpha"], [2, "beta"], [3, "gamma"]],
                sql("""
                    select id, MixedName
                    from ${topTable} for version as of ${topCpRename}
                    where metric >= 10
                    order by id
                """))
        assertUnknownColumn("""
            select old_name from ${topTable} for version as of ${topCpRename}
        """, "old_name")
        assertUnknownColumn("""
            select victim from ${topTable} for version as of ${topCpDrop}
        """, "victim")
        assertEquals([[1, null], [2, null], [3, null], [4, null], [5, 5000L]],
                sql("""
                    select id, victim
                    from ${topTable} for version as of ${topCpReadd}
                    order by id
                """))
        assertEquals([[6, 6000000000L]],
                sql("""
                    select id, metric
                    from ${topTable} for version as of ${topCpPromote}
                    where metric > 5000000000
                """))

        // Scenario TC02: nested refs validate STRUCT/MAP/ARRAY projection and predicate.
        assertEquals([[1, 10, 20, 30]],
                sql("""
                    select id, payload.old_child,
                           element_at(attributes, 'a').old_child,
                           events[1].old_child
                    from ${nestedTable} for version as of ${nestedCp0}
                    where payload.old_child = 10
                """))
        assertEquals([[1, null, null, null], [2, 112, 122, 132]],
                sql("""
                    select id, payload.added_child,
                           element_at(attributes, 'a').added_child,
                           events[1].added_child
                    from ${nestedTable} for version as of ${nestedCpAdd}
                    order by id
                """))
        assertEquals([[1, 10, 20, 30], [2, 110, 120, 130], [3, 210, 220, 230]],
                sql("""
                    select id, payload.renamed_child,
                           element_at(attributes, 'a').renamed_child,
                           events[1].renamed_child
                    from ${nestedTable} for version as of 'nested_cp_rename'
                    order by id
                """))
        assertUnknownColumn("""
            select payload.old_child
            from ${nestedTable} for version as of ${nestedCpRename}
        """, "old_child")
        assertEquals([[1, null], [2, null], [3, null], [4, 3100L]],
                sql("""
                    select id, payload.renamed_child
                    from ${nestedTable} for version as of ${nestedCpReadd}
                    order by id
                """))

        // Scenario TC05/S18: PK upsert/delete/DV results remain correct at old and new refs.
        assertEquals([[1, "alpha", "old-note-1", 10], [2, "beta", "old-note-2", 20]],
                sql("""
                    select id, old_name, note, score
                    from ${pkTable} for version as of 'pk_cp0'
                    order by id
                """))
        assertEquals([[1, "alpha-updated", "new-note-1", 11, "extra-1"]],
                sql("""
                    select id, full_name, note, score, extra
                    from ${pkTable} for version as of ${pkCpRenameDelete}
                    order by id
                """))
        assertEquals([[1, "alpha-updated", null], [3, "gamma", 3000L]],
                sql("""
                    select id, full_name, note
                    from ${pkTable} for version as of ${pkCpReadd}
                    order by id
                """))
        assertUnknownColumn("""
            select old_name from ${pkTable} for version as of ${pkCpRenameDelete}
        """, "old_name")

        // Scenario T14: incremental reads crossing a rename checkpoint bind the end schema.
        List<List<Object>> incrementalJni
        List<List<Object>> incrementalCpp
        sql """set force_jni_scanner=false"""
        sql """set enable_paimon_cpp_reader=false"""
        incrementalJni = sql("""
            select id, full_name, score
            from ${pkTable}@incr(
                'startSnapshotId'='${pkCp0}',
                'endSnapshotId'='${pkCpRenameDelete}'
            )
            order by id
        """)
        sql """set enable_paimon_cpp_reader=true"""
        incrementalCpp = sql("""
            select id, full_name, score
            from ${pkTable}@incr(
                'startSnapshotId'='${pkCp0}',
                'endSnapshotId'='${pkCpRenameDelete}'
            )
            order by id
        """)
        assertEquals(incrementalJni, incrementalCpp)

        // Scenario TC03/S16: partition pruning and renamed payloads bind to their own snapshots.
        assertEquals([[1, "p1", "old"], [2, "p2", "old"]],
                sql("""
                    select id, old_partition, payload
                    from ${partitionTable} for version as of ${partitionCp0}
                    where old_partition = 'p1' or old_partition = 'p2'
                    order by id
                """))
        assertEquals([[1, "p1", "old"], [2, "p2", "old"], [3, "p3", "new"]],
                sql("""
                    select id, old_partition, new_payload
                    from ${partitionTable} for version as of ${partitionCpRename}
                    order by id
                """))
        assertUnknownColumn("""
            select new_payload
            from ${partitionTable} for version as of ${partitionCp0}
        """, "new_payload")

        // Scenario TC08/S20: illegal PK/partition changes fail atomically.
        test {
            sql """alter table ${pkTable} drop column id"""
            exception "DROP COLUMN not supported"
        }
        test {
            sql """alter table ${partitionTable} drop column old_partition"""
            exception "DROP COLUMN not supported"
        }
        assertEquals([[1, "alpha-updated"], [3, "gamma"]],
                sql("""select id, full_name from ${pkTable} order by id"""))
        assertEquals([[1, "p1", "old"], [2, "p2", "old"], [3, "p3", "new"]],
                sql("""select id, old_partition, new_payload from ${partitionTable} order by id"""))

        // Scenario TC09/R13/R17: cache, JNI and CPP paths return the same historical schema/data.
        sql """switch ${noCacheCatalogName}"""
        sql """use ${dbName}"""
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of ${topCp0}
            order by id
        """))
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """set enable_paimon_cpp_reader=false"""
        sql """set force_jni_scanner=true"""
        List<List<Object>> forcedJniRows = sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of ${topCp0}
            order by id
        """)
        sql """set force_jni_scanner=false"""
        sql """set enable_paimon_cpp_reader=true"""
        List<List<Object>> cppRows = sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of ${topCp0}
            order by id
        """)
        assertEquals(forcedJniRows, cppRows)

        // Scenario T10/T11: retained tags survive expiration; missing refs never fall back to latest.
        spark_paimon """
            ALTER TABLE paimon.${dbName}.${topTable}
            SET TBLPROPERTIES ('snapshot.num-retained.min'='1')
        """
        spark_paimon """
            CALL paimon.sys.expire_snapshots(
                table => '${dbName}.${topTable}',
                retain_max => 1
            )
        """
        sql """refresh table ${topTable}"""
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of 'top_cp0'
            order by id
        """))
        test {
            sql """select * from ${topTable} for version as of 9223372036854775807"""
            exception "snapshot"
        }
        test {
            sql """select * from ${topTable} for version as of 'missing_schema_tag'"""
            exception "tag"
        }
    } finally {
        sql """set enable_paimon_cpp_reader=false"""
        sql """set force_jni_scanner=false"""
        sql """drop catalog if exists ${catalogName}"""
        sql """drop catalog if exists ${noCacheCatalogName}"""
    }
}
