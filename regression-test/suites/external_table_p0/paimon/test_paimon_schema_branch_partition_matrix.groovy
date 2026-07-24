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

suite("test_paimon_schema_branch_partition_matrix", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_schema_branch_partition_matrix"
    String dbName = "paimon_schema_branch_partition_db"
    String branchTable = "branch_schema_timeline"
    String partitionTable = "partition_schema_timeline"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
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
            create database if not exists paimon.${dbName};
            drop table if exists paimon.${dbName}.${branchTable};
            create table paimon.${dbName}.${branchTable} (
                id int,
                old_name string,
                metric int
            ) using paimon
            tblproperties ('file.format'='parquet');
            insert into paimon.${dbName}.${branchTable} values (1, 'base-1', 10);
            call paimon.sys.create_tag(
                table => '${dbName}.${branchTable}',
                tag => 'branch_base'
            );
            call paimon.sys.create_branch(
                '${dbName}.${branchTable}',
                'schema_branch',
                'branch_base'
            );
        """

        // Scenario T09-branch-evolution: branch owns an independent rename/type/add timeline.
        spark_paimon_multi """
            alter table paimon.${dbName}.`${branchTable}\$branch_schema_branch`
                rename column old_name to branch_name;
            alter table paimon.${dbName}.`${branchTable}\$branch_schema_branch`
                alter column metric type bigint;
            alter table paimon.${dbName}.`${branchTable}\$branch_schema_branch`
                add column branch_only string;
            insert into paimon.${dbName}.`${branchTable}\$branch_schema_branch`
                values (2, 'branch-2', 6000000000, 'branch-only-2');
        """

        // Main evolves independently after the branch is created.
        spark_paimon_multi """
            alter table paimon.${dbName}.${branchTable} add column main_only string;
            insert into paimon.${dbName}.${branchTable}
                values (3, 'main-3', 30, 'main-only-3');
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh table ${branchTable}"""

        assertEquals([[1, "base-1", 10, null], [3, "main-3", 30, "main-only-3"]], sql("""
            select id, old_name, metric, main_only
            from ${branchTable}
            order by id
        """))
        // The branch's independently-evolved schema is now read via its OWN schema dir: applySnapshot
        // routes @branch(schema_branch) to withBranch so the branch's schema-<id> resolves (old_name
        // renamed to branch_name, metric widened to bigint, branch_only added; row 1 inherited from the
        // base tag defaults branch_only=null).
        assertEquals([[1, "base-1", 10L, null], [2, "branch-2", 6000000000L, "branch-only-2"]], sql("""
            select id, branch_name, metric, branch_only
            from ${branchTable}@branch(schema_branch)
            order by id
        """))
        test {
            sql """select branch_only from ${branchTable}"""
            exception "Unknown column 'branch_only'"
        }

        // Scenario T09-fast-forward: branch schema and data replace main after Paimon fast-forward.
        spark_paimon """
            call paimon.sys.fast_forward('${dbName}.${branchTable}', 'schema_branch')
        """
        spark_paimon """refresh table paimon.${dbName}.${branchTable}"""
        sql """refresh table ${branchTable}"""
        assertEquals([
                [1, "base-1", 10L, null],
                [2, "branch-2", 6000000000L, "branch-only-2"]
        ], spark_paimon("""
            select id, branch_name, metric, branch_only
            from paimon.${dbName}.${branchTable}
            order by id
        """))
        List<String> fastForwardColumns = sql("""desc ${branchTable}""")
                .collect { row -> row[0].toString() }
        assertTrue(fastForwardColumns.containsAll(
                ["id", "branch_name", "metric", "branch_only"]))
        test {
            sql """select old_name from ${branchTable}"""
            exception "Unknown column 'old_name'"
        }

        spark_paimon_multi """
            drop table if exists paimon.${dbName}.${partitionTable};
            create table paimon.${dbName}.${partitionTable} (
                id int,
                part string,
                old_payload string
            ) using paimon
            partitioned by (part)
            tblproperties ('file.format'='orc');
            insert into paimon.${dbName}.${partitionTable}
                values (1, 'p1', 'old-1'), (2, 'p2', 'old-2');
            call paimon.sys.create_tag(
                table => '${dbName}.${partitionTable}',
                tag => 'partition_before_change'
            );
            alter table paimon.${dbName}.${partitionTable}
                rename column old_payload to new_payload;
            insert into paimon.${dbName}.${partitionTable}
                values (3, 'p3', 'new-3');
        """
        sql """refresh table ${partitionTable}"""

        // Scenario S16-supported: non-partition payload rename keeps old/new snapshot bindings.
        assertEquals([[1, "p1", "old-1"], [2, "p2", "old-2"]], sql("""
            select id, part, old_payload
            from ${partitionTable}@tag(partition_before_change)
            order by id
        """))
        assertEquals([
                [1, "p1", "old-1"],
                [2, "p2", "old-2"],
                [3, "p3", "new-3"]
        ], sql("""
            select id, part, new_payload from ${partitionTable} order by id
        """))

        // Scenario S16-negative: Paimon partition keys only support reordering.
        // The operation must fail atomically without changing schema, snapshots or partition data.
        int snapshotsBefore = spark_paimon("""
            select count(*)
            from paimon.${dbName}.`${partitionTable}\$snapshots`
        """)[0][0].toString().toInteger()
        boolean renameRejected = false
        try {
            spark_paimon """
                alter table paimon.${dbName}.${partitionTable}
                    rename column part to renamed_part
            """
        } catch (Exception e) {
            renameRejected = true
            assertTrue(e.message.toLowerCase().contains("partition"))
        }
        assertTrue(renameRejected, "Paimon must reject partition-key rename")

        boolean typeRejected = false
        try {
            spark_paimon """
                alter table paimon.${dbName}.${partitionTable}
                    alter column part type bigint
            """
        } catch (Exception e) {
            typeRejected = true
            assertTrue(e.message.toLowerCase().contains("partition"))
        }
        assertTrue(typeRejected, "Paimon must reject partition-key type changes")

        boolean dropRejected = false
        try {
            spark_paimon """
                alter table paimon.${dbName}.${partitionTable} drop column part
            """
        } catch (Exception e) {
            dropRejected = true
            assertTrue(e.message.toLowerCase().contains("partition"))
        }
        assertTrue(dropRejected, "Paimon must reject dropping a partition key")

        int snapshotsAfter = spark_paimon("""
            select count(*)
            from paimon.${dbName}.`${partitionTable}\$snapshots`
        """)[0][0].toString().toInteger()
        assertEquals(snapshotsBefore, snapshotsAfter)
        sql """refresh table ${partitionTable}"""
        assertEquals([
                [1, "p1", "old-1"],
                [2, "p2", "old-2"],
                [3, "p3", "new-3"]
        ], sql("""
            select id, part, new_payload from ${partitionTable} order by id
        """))
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
