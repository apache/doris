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

suite("test_iceberg_schema_time_travel_matrix",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_schema_time_travel_matrix"
    String noCacheCatalogName = "test_iceberg_schema_time_travel_matrix_no_cache"
    String dbName = "iceberg_schema_time_travel_matrix_db"
    String topTable = "top_timeline"
    String nestedTable = "nested_timeline"
    String dorisNestedTable = "doris_nested_timeline"
    String deleteTable = "delete_partition_timeline"

    def latestSnapshotId = { String tableName ->
        List<List<Object>> rows = spark_iceberg """
            SELECT snapshot_id
            FROM demo.${dbName}.${tableName}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
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
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1'
        )
    """
    sql """
        CREATE CATALOG ${noCacheCatalogName} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1',
            'meta.cache.iceberg.table.ttl-second'='0',
            'meta.cache.iceberg.schema.ttl-second'='0'
        )
    """

    try {
        spark_iceberg_multi """
            CREATE DATABASE IF NOT EXISTS demo.${dbName};
            DROP TABLE IF EXISTS demo.${dbName}.${topTable};
            CREATE TABLE demo.${dbName}.${topTable} (
                id INT,
                old_name STRING,
                victim STRING,
                metric INT
            ) USING iceberg
            TBLPROPERTIES (
                'format-version'='2',
                'write.format.default'='parquet'
            );
            INSERT INTO demo.${dbName}.${topTable}
                VALUES (1, 'alpha', 'old-v1', 10);
        """
        String topCp0 = latestSnapshotId(topTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${topTable}` CREATE TAG top_cp0"""
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${topTable}` CREATE BRANCH top_cp0_branch"""
        Thread.sleep(1100)

        // Scenario S01/S02/S03 x T00/T01/T02/T05/T08:
        // add and reorder columns while keeping the pre-change snapshot, tag and branch readable.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${topTable}
                ADD COLUMNS (added STRING AFTER old_name, added_second BIGINT);
            ALTER TABLE demo.${dbName}.${topTable} ALTER COLUMN added_second FIRST;
            INSERT INTO demo.${dbName}.${topTable}
                (id, old_name, victim, metric, added, added_second)
                VALUES (2, 'beta', 'old-v2', 20, 'added-v2', 200);
        """
        String topCpAdd = latestSnapshotId(topTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${topTable}` CREATE TAG top_cp_add"""
        Thread.sleep(1100)

        // Scenario S04/S05 x T00-T08:
        // a rename must be observable through explicit old/new names, not only SELECT *.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${topTable} RENAME COLUMN old_name TO MixedName;
            INSERT INTO demo.${dbName}.${topTable}
                (id, MixedName, victim, metric, added, added_second)
                VALUES (3, 'gamma', 'old-v3', 30, 'added-v3', 300);
        """
        String topCpRename = latestSnapshotId(topTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${topTable}` CREATE TAG top_cp_rename"""
        Thread.sleep(1100)

        // Scenario S06 x T00/T01/T02/T05: old refs retain the dropped field.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${topTable} DROP COLUMN victim;
            INSERT INTO demo.${dbName}.${topTable}
                (id, MixedName, metric, added, added_second)
                VALUES (4, 'delta', 40, 'added-v4', 400);
        """
        String topCpDrop = latestSnapshotId(topTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${topTable}` CREATE TAG top_cp_drop"""
        Thread.sleep(1100)

        // Scenario S07 x T00/T01/T02/T05/T12/T13:
        // reusing the name with a new BIGINT field ID must never expose old STRING values.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${topTable} ADD COLUMN victim BIGINT;
            INSERT INTO demo.${dbName}.${topTable}
                (id, MixedName, metric, added, added_second, victim)
                VALUES (5, 'epsilon', 50, 'added-v5', 500, 5000);
        """
        String topCpReadd = latestSnapshotId(topTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${topTable}` CREATE TAG top_cp_readd"""
        Thread.sleep(1100)

        // Scenario S08 x T00/T01/T02/T05: compatible INT -> BIGINT promotion.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${topTable} ALTER COLUMN metric TYPE BIGINT;
            INSERT INTO demo.${dbName}.${topTable}
                (id, MixedName, metric, added, added_second, victim)
                VALUES (6, 'zeta', 6000000000, 'added-v6', 600, 6000);
        """
        String topCpPromote = latestSnapshotId(topTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${topTable}` CREATE TAG top_cp_promote"""

        spark_iceberg_multi """
            DROP TABLE IF EXISTS demo.${dbName}.${nestedTable};
            CREATE TABLE demo.${dbName}.${nestedTable} (
                id INT,
                payload STRUCT<old_child: INT, keep: INT>,
                attributes MAP<STRING, STRUCT<old_child: INT, keep: INT>>,
                events ARRAY<STRUCT<old_child: INT, keep: INT>>
            ) USING iceberg
            TBLPROPERTIES ('format-version'='2', 'write.format.default'='orc');
            INSERT INTO demo.${dbName}.${nestedTable} VALUES (
                1,
                named_struct('old_child', 10, 'keep', 11),
                map('a', named_struct('old_child', 20, 'keep', 21)),
                array(named_struct('old_child', 30, 'keep', 31))
            );
        """
        String nestedCp0 = latestSnapshotId(nestedTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${nestedTable}` CREATE TAG nested_cp0"""

        // Scenario S09/S14/S15 x T00/T01/T02/T05:
        // add children to STRUCT, MAP value struct and ARRAY element struct.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${nestedTable} ADD COLUMN payload.added_child INT;
            ALTER TABLE demo.${dbName}.${nestedTable} ADD COLUMN attributes.value.added_child INT;
            ALTER TABLE demo.${dbName}.${nestedTable} ADD COLUMN events.element.added_child INT;
            INSERT INTO demo.${dbName}.${nestedTable} VALUES (
                2,
                named_struct('old_child', 110, 'keep', 111, 'added_child', 112),
                map('a', named_struct('old_child', 120, 'keep', 121, 'added_child', 122)),
                array(named_struct('old_child', 130, 'keep', 131, 'added_child', 132))
            );
        """
        String nestedCpAdd = latestSnapshotId(nestedTable)

        // Scenario S10/S14/S15 x T00/T01/T02/T05:
        // rename nested children and verify both positive and negative binding.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${nestedTable}
                RENAME COLUMN payload.old_child TO renamed_child;
            ALTER TABLE demo.${dbName}.${nestedTable}
                RENAME COLUMN attributes.value.old_child TO renamed_child;
            ALTER TABLE demo.${dbName}.${nestedTable}
                RENAME COLUMN events.element.old_child TO renamed_child;
            INSERT INTO demo.${dbName}.${nestedTable} VALUES (
                3,
                named_struct('renamed_child', 210, 'keep', 211, 'added_child', 212),
                map('a', named_struct('renamed_child', 220, 'keep', 221, 'added_child', 222)),
                array(named_struct('renamed_child', 230, 'keep', 231, 'added_child', 232))
            );
        """
        String nestedCpRename = latestSnapshotId(nestedTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${nestedTable}` CREATE TAG nested_cp_rename"""

        // Scenario S11/S12/S13 x T00/T01/T02/T05:
        // drop/re-add a nested name with a new ID and promote the surviving child type.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${nestedTable} DROP COLUMN payload.renamed_child;
            ALTER TABLE demo.${dbName}.${nestedTable} DROP COLUMN attributes.value.renamed_child;
            ALTER TABLE demo.${dbName}.${nestedTable} DROP COLUMN events.element.renamed_child;
            ALTER TABLE demo.${dbName}.${nestedTable} ADD COLUMN payload.renamed_child BIGINT;
            ALTER TABLE demo.${dbName}.${nestedTable} ADD COLUMN attributes.value.renamed_child BIGINT;
            ALTER TABLE demo.${dbName}.${nestedTable} ADD COLUMN events.element.renamed_child BIGINT;
            ALTER TABLE demo.${dbName}.${nestedTable} ALTER COLUMN payload.keep TYPE BIGINT;
            INSERT INTO demo.${dbName}.${nestedTable} VALUES (
                4,
                named_struct('keep', 311, 'added_child', 312, 'renamed_child', 3100),
                map('a', named_struct('keep', 321, 'added_child', 322, 'renamed_child', 3200)),
                array(named_struct('keep', 331, 'added_child', 332, 'renamed_child', 3300))
            );
        """
        String nestedCpReadd = latestSnapshotId(nestedTable)

        spark_iceberg_multi """
            SET spark.sql.shuffle.partitions=1;
            DROP TABLE IF EXISTS demo.${dbName}.${deleteTable};
            CREATE TABLE demo.${dbName}.${deleteTable} (
                id INT,
                old_name STRING,
                category STRING,
                event_time TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (days(event_time))
            TBLPROPERTIES (
                'format-version'='2',
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read',
                'write.distribution-mode'='none',
                'write.target-file-size-bytes'='134217728'
            );
            INSERT INTO demo.${dbName}.${deleteTable}
            SELECT /*+ COALESCE(1) */ id, old_name, category, event_time FROM VALUES
                (1, 'a', 'x', TIMESTAMP '2026-01-01 01:00:00'),
                (2, 'b', 'x', TIMESTAMP '2026-01-01 02:00:00'),
                (3, 'c', 'y', TIMESTAMP '2026-01-02 01:00:00')
                AS t(id, old_name, category, event_time);
        """
        String deleteCp0 = latestSnapshotId(deleteTable)
        sql """ALTER TABLE `${catalogName}`.`${dbName}`.`${deleteTable}` CREATE TAG delete_cp0"""

        // Scenario S04/S17 x TC03/TC04:
        // position deletes after rename and partition-spec evolution must not affect the old ref.
        spark_iceberg_multi """
            ALTER TABLE demo.${dbName}.${deleteTable} RENAME COLUMN old_name TO new_name;
            ALTER TABLE demo.${dbName}.${deleteTable} ADD PARTITION FIELD bucket(2, id);
            INSERT INTO demo.${dbName}.${deleteTable} VALUES
                (4, 'd', 'y', TIMESTAMP '2026-01-02 02:00:00');
            DELETE FROM demo.${dbName}.${deleteTable} WHERE id = 2;
        """
        String deleteCpAfter = latestSnapshotId(deleteTable)

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh catalog ${catalogName}"""

        // Scenario S09-S15 x T00-T13 x TC02/TC04:
        // exercise the nested Iceberg DDL implemented on the latest master through Doris itself,
        // then combine it with snapshots, tags, a branch, dual-snapshot binding and a delete.
        sql """drop table if exists ${dorisNestedTable}"""
        sql """
            create table ${dorisNestedTable} (
                id int,
                info struct<metric:int, label:string>,
                events array<struct<score:int>>,
                attrs map<string, struct<code:int>>
            )
        """
        sql """
            insert into ${dorisNestedTable} values (
                1,
                struct(10, 'old-info'),
                array(struct(100)),
                map('k', struct(1000))
            )
        """
        String dorisNestedCp0 = sql("""
            select snapshot_id
            from ${dorisNestedTable}\$snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
        sql """
            alter table ${dorisNestedTable}
            create tag doris_nested_cp0 as of version ${dorisNestedCp0}
        """
        sql """
            alter table ${dorisNestedTable}
            create branch doris_nested_cp0_branch as of version ${dorisNestedCp0}
        """
        Thread.sleep(1100)

        // Scenario S09/S14/S15: Doris adds fields at nested STRUCT, ARRAY element and MAP value paths.
        sql """
            alter table ${dorisNestedTable}
            add column info.added string null after metric
        """
        sql """
            alter table ${dorisNestedTable}
            add column events.element.added int null after score
        """
        sql """
            alter table ${dorisNestedTable}
            add column attrs.value.added int null after code
        """
        sql """
            insert into ${dorisNestedTable} values (
                2,
                struct(20, 'info-added', 'after-add'),
                array(struct(200, 201)),
                map('k', struct(2000, 2001))
            )
        """
        String dorisNestedCpAdd = sql("""
            select snapshot_id
            from ${dorisNestedTable}\$snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
        sql """
            alter table ${dorisNestedTable}
            create tag doris_nested_cp_add as of version ${dorisNestedCpAdd}
        """
        Thread.sleep(1100)

        // Scenario S10/S13/S14/S15: rename nested fields and promote sibling field types in one timeline.
        sql """alter table ${dorisNestedTable} rename column info.added to renamed"""
        sql """
            alter table ${dorisNestedTable}
            rename column events.element.added to renamed
        """
        sql """
            alter table ${dorisNestedTable}
            rename column attrs.value.added to renamed
        """
        sql """alter table ${dorisNestedTable} modify column info.metric bigint"""
        sql """
            alter table ${dorisNestedTable}
            modify column events.element.score bigint
        """
        sql """
            alter table ${dorisNestedTable}
            modify column attrs.value.code bigint
        """
        sql """
            insert into ${dorisNestedTable} values (
                3,
                struct(cast(30 as bigint), 'info-renamed', 'after-rename'),
                array(struct(cast(300 as bigint), 301)),
                map('k', struct(cast(3000 as bigint), 3001))
            )
        """
        String dorisNestedCpRename = sql("""
            select snapshot_id
            from ${dorisNestedTable}\$snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
        sql """
            alter table ${dorisNestedTable}
            create tag doris_nested_cp_rename as of version ${dorisNestedCpRename}
        """
        Thread.sleep(1100)

        // Scenario S11/S12/S14/S15: re-add the same nested names with new BIGINT field IDs.
        sql """alter table ${dorisNestedTable} drop column info.renamed"""
        sql """alter table ${dorisNestedTable} drop column events.element.renamed"""
        sql """alter table ${dorisNestedTable} drop column attrs.value.renamed"""
        sql """alter table ${dorisNestedTable} add column info.renamed bigint null"""
        sql """
            alter table ${dorisNestedTable}
            add column events.element.renamed bigint null
        """
        sql """
            alter table ${dorisNestedTable}
            add column attrs.value.renamed bigint null
        """
        sql """
            insert into ${dorisNestedTable} values (
                4,
                struct(cast(40 as bigint), 'info-readd', cast(4001 as bigint)),
                array(struct(cast(400 as bigint), cast(401 as bigint))),
                map('k', struct(cast(4000 as bigint), cast(4001 as bigint)))
            )
        """
        String dorisNestedCpReadd = sql("""
            select snapshot_id
            from ${dorisNestedTable}\$snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
        sql """
            alter table ${dorisNestedTable}
            create tag doris_nested_cp_readd as of version ${dorisNestedCpReadd}
        """

        // Scenario TC04: a delete after nested evolution must not change any older snapshot/tag.
        sql """delete from ${dorisNestedTable} where id = 2"""
        String dorisNestedCpDelete = sql("""
            select snapshot_id
            from ${dorisNestedTable}\$snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()

        // Scenario TC02/T01/T05/T06/T08: every old reference exposes its own nested schema.
        assertEquals([[1, 10, 100, 1000]],
                sql("""
                    select id, info.metric, events[1].score, attrs['k'].code
                    from ${dorisNestedTable} for version as of ${dorisNestedCp0}
                    order by id
                """))
        assertEquals([[1, 10, 100, 1000]],
                sql("""
                    select id, info.metric, events[1].score, attrs['k'].code
                    from ${dorisNestedTable} for version as of 'doris_nested_cp0'
                    order by id
                """))
        assertEquals([[1, 10, 100, 1000]],
                sql("""
                    select id, info.metric, events[1].score, attrs['k'].code
                    from ${dorisNestedTable}@tag(doris_nested_cp0)
                    order by id
                """))
        // Known product issue DORIS-27425: an old branch leaks the latest BIGINT nested types.
        assertEquals([[1, 10L, 100L, 1000L]],
                sql("""
                    select id, info.metric, events[1].score, attrs['k'].code
                    from ${dorisNestedTable}@branch(doris_nested_cp0_branch)
                    order by id
                """))
        assertUnknownColumn("""
            select info.renamed
            from ${dorisNestedTable} for version as of ${dorisNestedCp0}
        """, "renamed")

        // Scenario TC02/T03/T04: complex-field time travel uses the pre-change nested schema.
        List<List<Object>> dorisNestedCp0Time = sql("""
            select date_format(date_add(committed_at, interval 1 second), '%Y-%m-%d %H:%i:%s'),
                   cast(unix_timestamp(committed_at) * 1000 + 999 as bigint)
            from ${dorisNestedTable}\$snapshots
            where snapshot_id = ${dorisNestedCp0}
        """)
        assertEquals([[1, 10]],
                sql("""
                    select id, info.metric
                    from ${dorisNestedTable}
                    for time as of "${dorisNestedCp0Time[0][0]}"
                    order by id
                """))
        // Scenario TC03-negative: Iceberg accepts time strings, not Paimon-style epoch millis.
        test {
            sql """
                select id, info.metric
                from ${dorisNestedTable}
                for time as of ${dorisNestedCp0Time[0][1]}
                order by id
            """
            exception "can't parse time"
        }

        // Scenario TC02: nested add/rename/drop-readd checkpoints verify projection and predicates.
        assertEquals([[1, null, null, null], [2, "info-added", 201, 2001]],
                sql("""
                    select id, info.added, events[1].added, attrs['k'].added
                    from ${dorisNestedTable} for version as of ${dorisNestedCpAdd}
                    order by id
                """))
        assertEquals([[1, null, null, null], [2, "info-added", 201, 2001],
                      [3, "info-renamed", 301, 3001]],
                sql("""
                    select id, info.renamed, events[1].renamed, attrs['k'].renamed
                    from ${dorisNestedTable} for version as of ${dorisNestedCpRename}
                    where info.metric >= 10
                    order by id
                """))
        assertUnknownColumn("""
            select info.added
            from ${dorisNestedTable} for version as of ${dorisNestedCpRename}
        """, "added")
        assertEquals([[1, null, null, null], [2, null, null, null],
                      [3, null, null, null], [4, 4001L, 401L, 4001L]],
                sql("""
                    select id, info.renamed, events[1].renamed, attrs['k'].renamed
                    from ${dorisNestedTable} for version as of ${dorisNestedCpReadd}
                    order by id
                """))

        // Scenario TC07/T12/T13, known product issue DORIS-27427:
        // two Iceberg historical relations incorrectly reuse the first nested schema.
        test {
            sql """
                select id, info.added as nested_value, info.added as duplicate_value
                from ${dorisNestedTable} for version as of ${dorisNestedCpAdd}
                union all
                select id, info.renamed as nested_value, info.renamed as duplicate_value
                from ${dorisNestedTable} for version as of ${dorisNestedCpRename}
                order by id, nested_value
            """
            exception "No such struct field 'renamed'"
        }
        test {
            sql """
                select old_side.id, old_side.added_value, new_side.renamed_value
                from (
                    select id, info.added as added_value
                    from ${dorisNestedTable} for version as of ${dorisNestedCpAdd}
                ) old_side
                join (
                    select id, info.renamed as renamed_value
                    from ${dorisNestedTable} for version as of ${dorisNestedCpRename}
                ) new_side on old_side.id = new_side.id
                order by old_side.id
            """
            exception "No such struct field 'renamed'"
        }

        // Scenario TC04: delete is visible only at/after the delete snapshot.
        assertEquals([1, 2, 3, 4],
                sql("""
                    select id
                    from ${dorisNestedTable} for version as of ${dorisNestedCpReadd}
                    order by id
                """).collect { it[0] })
        assertEquals([1, 3, 4],
                sql("""
                    select id
                    from ${dorisNestedTable} for version as of ${dorisNestedCpDelete}
                    order by id
                """).collect { it[0] })

        // Scenario TC01: current schema uses only the new names and promoted/re-added types.
        List<String> currentColumns = sql("""desc ${topTable}""").collect { it[0].toString() }
        assertEquals(["added_second", "id", "MixedName", "added", "metric", "victim"], currentColumns)
        assertEquals([[1, null], [2, null], [3, null], [4, null], [5, 5000L], [6, 6000L]],
                sql("""select id, victim from ${topTable} order by id"""))
        assertEquals([[6, 6000000000L]],
                sql("""select id, metric from ${topTable} where metric > 5000000000 order by id"""))
        assertUnknownColumn("""select old_name from ${topTable}""", "old_name")

        // Scenario T01/T05/T06/T08: the pre-change snapshot/tag/branch binds old_name and victim.
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
        // Known product issue DORIS-27425: an old branch is analyzed with the latest rename schema.
        test {
            sql """
                select id, old_name, victim, metric
                from ${topTable}@branch(top_cp0_branch)
                order by id
            """
            exception "Unknown column 'old_name'"
        }
        assertUnknownColumn("""
            select MixedName from ${topTable} for version as of ${topCp0}
        """, "MixedName")

        // Scenario T03/T04: validate string time travel and reject unsupported epoch millis.
        List<List<Object>> cp0TimeRows = sql("""
            select date_format(date_add(committed_at, interval 1 second), '%Y-%m-%d %H:%i:%s'),
                   cast(unix_timestamp(committed_at) * 1000 + 999 as bigint)
            from ${topTable}\$snapshots
            where snapshot_id = ${topCp0}
        """)
        assertEquals(1, cp0TimeRows.size())
        assertEquals(topCp0Rows, sql("""
            select id, old_name, victim, metric
            from ${topTable} for time as of "${cp0TimeRows[0][0]}"
            order by id
        """))
        test {
            sql """
                select id, old_name, victim, metric
                from ${topTable} for time as of ${cp0TimeRows[0][1]}
                order by id
            """
            exception "can't parse time"
        }

        // Scenario S01-S05: every checkpoint verifies projection, predicate and aggregation.
        assertEquals([[1, "alpha", null], [2, "beta", "added-v2"]],
                sql("""
                    select id, old_name, added
                    from ${topTable} for version as of ${topCpAdd}
                    where metric >= 10
                    order by id
                """))
        assertEquals([[3L, 60L]],
                sql("""
                    select count(*), sum(metric)
                    from ${topTable} for version as of ${topCpRename}
                """))
        assertUnknownColumn("""
            select old_name from ${topTable} for version as of ${topCpRename}
        """, "old_name")
        assertEquals([[1, "old-v1"], [2, "old-v2"], [3, "old-v3"]],
                sql("""
                    select id, victim
                    from ${topTable} for version as of 'top_cp_rename'
                    where victim like 'old-%'
                    order by id
                """))
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

        // Scenario TC07/T12/T13, known product issue DORIS-27427:
        // self-join and UNION incorrectly reuse one Iceberg historical schema.
        test {
            sql """
                select old_side.id, old_side.old_name, new_side.MixedName
                from (
                    select id, old_name
                    from ${topTable} for version as of ${topCp0}
                ) old_side
                join (
                    select id, MixedName
                    from ${topTable} for version as of ${topCpRename}
                ) new_side on old_side.id = new_side.id
                order by old_side.id
            """
            exception "Unknown column 'MixedName'"
        }
        test {
            sql """
                select id, old_name as name_value
                from ${topTable} for version as of ${topCp0}
                union all
                select id, MixedName as name_value
                from ${topTable} for version as of ${topCpRename}
                order by id, name_value
            """
            exception "Unknown column 'MixedName'"
        }

        // Scenario TC02: nested projection/predicate use each snapshot's nested field IDs.
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

        // Scenario TC03/TC04: delete visibility and renamed delete-table fields are snapshot-local.
        assertEquals([[1, "a"], [2, "b"], [3, "c"]],
                sql("""
                    select id, old_name
                    from ${deleteTable} for version as of 'delete_cp0'
                    order by id
                """))
        assertEquals([[1, "a"], [3, "c"], [4, "d"]],
                sql("""select id, new_name from ${deleteTable} order by id"""))
        assertEquals([[1, "a"], [3, "c"], [4, "d"]],
                sql("""
                    select id, new_name
                    from ${deleteTable} for version as of ${deleteCpAfter}
                    order by id
                """))
        assertTrue(((Number) sql("""
            select count(*) from ${deleteTable}\$position_deletes
        """)[0][0]).longValue() > 0L)

        // Scenario TC08/S20: illegal narrowing and dropping a partition source are atomic failures.
        test {
            sql """alter table ${topTable} modify column metric int"""
            exception "Cannot"
        }
        test {
            sql """alter table ${deleteTable} drop column event_time"""
            exception "Cannot"
        }
        assertEquals([[6, 6000000000L]],
                sql("""select id, metric from ${topTable} where id = 6"""))
        assertEquals([[1, "a"], [3, "c"], [4, "d"]],
                sql("""select id, new_name from ${deleteTable} order by id"""))

        // Scenario TC09/R13/R17: cache-off and scanner V1/V2 must produce identical historical rows.
        sql """switch ${noCacheCatalogName}"""
        sql """use ${dbName}"""
        List<List<Object>> noCacheRows = sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of ${topCp0}
            order by id
        """)
        assertEquals(topCp0Rows, noCacheRows)
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """set enable_file_scanner_v2=false"""
        List<List<Object>> legacyRows = sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of ${topCp0}
            order by id
        """)
        sql """set enable_file_scanner_v2=true"""
        List<List<Object>> v2Rows = sql("""
            select id, old_name, victim, metric
            from ${topTable} for version as of ${topCp0}
            order by id
        """)
        assertEquals(legacyRows, v2Rows)

        // Scenario T11: an unknown snapshot/tag must fail instead of silently reading latest.
        test {
            sql """select * from ${topTable} for version as of 9223372036854775807"""
            exception "does not have snapshotId 9223372036854775807"
        }
        test {
            sql """select * from ${topTable} for version as of 'missing_schema_tag'"""
            exception "does not have tag or branch named missing_schema_tag"
        }
    } finally {
        sql """set enable_file_scanner_v2=false"""
        sql """drop catalog if exists ${catalogName}"""
        sql """drop catalog if exists ${noCacheCatalogName}"""
    }
}
