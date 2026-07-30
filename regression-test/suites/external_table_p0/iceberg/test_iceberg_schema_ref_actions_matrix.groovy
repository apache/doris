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

suite("test_iceberg_schema_ref_actions_matrix",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_schema_ref_actions_matrix"
    String dbName = "iceberg_schema_ref_actions_db"

    def snapshots = { String tableName ->
        return sql("""
            select snapshot_id
            from ${tableName}\$snapshots
            order by committed_at, snapshot_id
        """).collect { row -> row[0].toString() }
    }

    def assertUnknownColumn = { String query, String columnName ->
        test {
            sql query
            exception "'${columnName}'"
        }
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1'
        )
    """
    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    try {
        // Scenario T15: rollback_to_snapshot rolls data back while the current schema stays latest.
        String rollbackTable = "rollback_schema_timeline"
        sql """drop table if exists ${rollbackTable}"""
        sql """
            create table ${rollbackTable} (
                id int,
                old_name string,
                payload struct<old_child:int, keep:int>
            ) engine=iceberg
            properties ('format-version'='2', 'write.format.default'='parquet')
        """
        sql """
            insert into ${rollbackTable}
            values (1, 'old-1', named_struct('old_child', 10, 'keep', 11))
        """
        String rollbackOldSnapshot = snapshots(rollbackTable).last()
        sql """alter table ${rollbackTable} create tag rollback_old"""

        sql """alter table ${rollbackTable} rename column old_name new_name"""
        sql """alter table ${rollbackTable} rename column payload.old_child new_child"""
        sql """
            insert into ${rollbackTable}
            values (2, 'new-2', named_struct('new_child', 20, 'keep', 21))
        """
        String rollbackNewSnapshot = snapshots(rollbackTable).last()
        sql """alter table ${rollbackTable} create tag rollback_new"""

        assertEquals([[1, "old-1", 10]], sql("""
            select id, old_name, payload.old_child
            from ${rollbackTable} for version as of ${rollbackOldSnapshot}
            order by id
        """))
        assertEquals([[1, "old-1", 10], [2, "new-2", 20]], sql("""
            select id, new_name, payload.new_child
            from ${rollbackTable} for version as of ${rollbackNewSnapshot}
            order by id
        """))

        sql """
            alter table ${rollbackTable}
            execute rollback_to_snapshot('snapshot_id'='${rollbackOldSnapshot}')
        """
        sql """refresh table ${rollbackTable}"""
        assertEquals([[1, "old-1", 10]], sql("""
            select id, new_name, payload.new_child
            from ${rollbackTable}
            order by id
        """))
        assertUnknownColumn("""select old_name from ${rollbackTable}""", "old_name")
        assertEquals([[1, "old-1", 10]], sql("""
            select id, old_name, payload.old_child
            from ${rollbackTable}@tag(rollback_old)
            order by id
        """))
        assertEquals([[1, "old-1", 10], [2, "new-2", 20]], sql("""
            select id, new_name, payload.new_child
            from ${rollbackTable}@tag(rollback_new)
            order by id
        """))

        // Scenario T16-cherrypick: append snapshot with the renamed schema can be replayed after rollback.
        sql """
            alter table ${rollbackTable}
            execute cherrypick_snapshot('snapshot_id'='${rollbackNewSnapshot}')
        """
        sql """refresh table ${rollbackTable}"""
        assertEquals([[1, "old-1", 10], [2, "new-2", 20]], sql("""
            select id, new_name, payload.new_child
            from ${rollbackTable}
            order by id
        """))
        assertEquals([[1, "old-1", 10]], sql("""
            select id, old_name, payload.old_child
            from ${rollbackTable}@tag(rollback_old)
            order by id
        """))

        // Scenario T15-timestamp: timestamp rollback also keeps the latest current schema.
        String timestampTable = "rollback_timestamp_schema_timeline"
        sql """drop table if exists ${timestampTable}"""
        sql """
            create table ${timestampTable} (
                id int,
                old_name string
            ) engine=iceberg
            properties ('format-version'='2', 'write.format.default'='orc')
        """
        sql """insert into ${timestampTable} values (1, 'old-1')"""
        List<List<Object>> timestampCheckpoint = sql("""
            select snapshot_id,
                   date_format(date_add(committed_at, interval 1 second),
                               '%Y-%m-%d %H:%i:%s.000')
            from ${timestampTable}\$snapshots
            order by committed_at desc
            limit 1
        """)
        String timestampOldSnapshot = timestampCheckpoint[0][0].toString()
        String rollbackTimestamp = timestampCheckpoint[0][1].toString()
        sql """alter table ${timestampTable} create tag timestamp_old"""
        Thread.sleep(1100)
        sql """alter table ${timestampTable} rename column old_name new_name"""
        sql """insert into ${timestampTable} values (2, 'new-2')"""

        sql """
            alter table ${timestampTable}
            execute rollback_to_timestamp('timestamp'='${rollbackTimestamp}')
        """
        sql """refresh table ${timestampTable}"""
        assertEquals([[1, "old-1"]], sql("""
            select id, new_name from ${timestampTable} order by id
        """))
        assertEquals([[1, "old-1"]], sql("""
            select id, old_name
            from ${timestampTable} for version as of ${timestampOldSnapshot}
            order by id
        """))

        // Scenario T16-fast-forward: advance a pre-rename branch to the renamed main schema.
        String fastForwardTable = "fast_forward_schema_timeline"
        sql """drop table if exists ${fastForwardTable}"""
        sql """
            create table ${fastForwardTable} (
                id int,
                old_name string,
                metric int
            ) engine=iceberg
            properties ('format-version'='2', 'write.format.default'='parquet')
        """
        sql """insert into ${fastForwardTable} values (1, 'old-1', 10)"""
        sql """alter table ${fastForwardTable} create branch pre_rename_branch"""
        sql """alter table ${fastForwardTable} create tag pre_rename_tag"""
        sql """alter table ${fastForwardTable} rename column old_name new_name"""
        sql """alter table ${fastForwardTable} modify column metric bigint"""
        sql """insert into ${fastForwardTable} values (2, 'new-2', 6000000000)"""

        // Scenario T08 negative contract: before fast-forward, branch reads use the latest rename schema.
        test {
            sql """
                select id, old_name, metric
                from ${fastForwardTable}@branch(pre_rename_branch)
                order by id
            """
            exception "Unknown column 'old_name'"
        }
        assertEquals([[1, "old-1", 10]], sql("""
            select id, old_name, metric
            from ${fastForwardTable}@tag(pre_rename_tag)
            order by id
        """))

        // Scenario T09 negative contract: a pre-rename branch write uses main's latest schema.
        test {
            sql """
                insert into ${fastForwardTable}@branch(pre_rename_branch)
                (id, old_name, metric) values (3, 'branch-3', 30)
            """
            exception "Unknown column 'old_name'"
        }

        sql """
            alter table ${fastForwardTable}
            execute fast_forward('branch'='pre_rename_branch', 'to'='main')
        """
        sql """refresh table ${fastForwardTable}"""
        assertEquals([[1, "old-1", 10L], [2, "new-2", 6000000000L]], sql("""
            select id, new_name, metric
            from ${fastForwardTable}@branch(pre_rename_branch)
            order by id
        """))
        assertEquals([[1, "old-1", 10]], sql("""
            select id, old_name, metric
            from ${fastForwardTable}@tag(pre_rename_tag)
            order by id
        """))
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
