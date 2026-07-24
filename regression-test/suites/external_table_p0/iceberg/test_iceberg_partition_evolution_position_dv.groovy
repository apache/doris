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

suite("test_iceberg_partition_evolution_position_dv",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_partition_evolution_position_dv"
    String dbName = "iceberg_partition_evolution_position_dv_db"

    def stringRows = { String query ->
        sql(query).collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }
    def latestSnapshotId = { String tableName ->
        return spark_iceberg("""
            select snapshot_id
            from demo.${dbName}.${tableName}.snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
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
            's3.region'='us-east-1',
            'meta.cache.iceberg.table.ttl-second'='0'
        )
    """

    try {
        ["parquet", "orc"].each { String format ->
            [2, 3].each { int formatVersion ->
                String deleteKind = formatVersion == 2 ? "position" : "dv"
                String tableName = "${deleteKind}_${format}_evolved"

                spark_iceberg_multi """
                    create database if not exists demo.${dbName};
                    drop table if exists demo.${dbName}.${tableName};
                    create table demo.${dbName}.${tableName} (
                        id int,
                        category string,
                        event_time timestamp,
                        payload struct<metric:int, label:string>
                    ) using iceberg
                    partitioned by (category, days(event_time))
                    tblproperties (
                        'format-version'='${formatVersion}',
                        'write.format.default'='${format}',
                        'write.delete.mode'='merge-on-read',
                        'write.update.mode'='merge-on-read',
                        'write.merge.mode'='merge-on-read',
                        'write.distribution-mode'='none'
                    );
                    insert into demo.${dbName}.${tableName}
                    select /*+ coalesce(1) */ id, category, event_time, payload from values
                        (1, 'A', timestamp '2026-01-01 01:00:00',
                            named_struct('metric', 10, 'label', 'base-a')),
                        (2, 'A', timestamp '2026-01-01 02:00:00',
                            named_struct('metric', 20, 'label', 'base-delete')),
                        (3, 'B', timestamp '2026-01-02 01:00:00',
                            named_struct('metric', 30, 'label', 'base-b')),
                        (4, 'C', timestamp '2026-01-03 01:00:00',
                            named_struct('metric', 40, 'label', 'base-c'))
                        as t(id, category, event_time, payload);
                """
                String baseSnapshot = latestSnapshotId(tableName)
                sql """
                    alter table `${catalogName}`.`${dbName}`.`${tableName}`
                    create tag ${tableName}_base as of version ${baseSnapshot}
                """

                // Scenario PE-D01: delete against the original spec before any evolution.
                spark_iceberg """
                    delete from demo.${dbName}.${tableName} where id = 2
                """
                String firstDeleteSnapshot = latestSnapshotId(tableName)

                // Scenario PE-D02: ADD partition field and complex child, then delete a row written
                // with the new spec. Old/new delete files must remain associated with their specs.
                spark_iceberg_multi """
                    alter table demo.${dbName}.${tableName} add partition field bucket(8, id);
                    alter table demo.${dbName}.${tableName} add column payload.extra string;
                    insert into demo.${dbName}.${tableName}
                    select /*+ coalesce(1) */ id, category, event_time, payload from values
                        (5, 'A', timestamp '2026-02-01 01:00:00',
                            named_struct('metric', 50, 'label', 'add-a', 'extra', 'new-child')),
                        (6, 'A', timestamp '2026-02-01 02:00:00',
                            named_struct('metric', 60, 'label', 'add-delete', 'extra', 'new-child'))
                        as t(id, category, event_time, payload);
                    delete from demo.${dbName}.${tableName} where id = 6;
                """
                String addedDeleteSnapshot = latestSnapshotId(tableName)

                // Scenario PE-D03: REPLACE temporal transform and rename/promote nested fields.
                spark_iceberg_multi """
                    alter table demo.${dbName}.${tableName}
                        replace partition field days(event_time) with months(event_time);
                    alter table demo.${dbName}.${tableName}
                        rename column payload.label to renamed_label;
                    alter table demo.${dbName}.${tableName}
                        alter column payload.metric type bigint;
                    insert into demo.${dbName}.${tableName} values
                        (7, 'A', timestamp '2026-03-01 01:00:00',
                            named_struct('metric', 7000000000,
                                'renamed_label', 'replace-a', 'extra', 'renamed-child')),
                        (8, 'A', timestamp '2026-03-01 02:00:00',
                            named_struct('metric', 80,
                                'renamed_label', 'replace-delete', 'extra', 'renamed-child'));
                """

                // Scenario PE-D04: DROP identity field before the final delete. The matching row
                // is in a spec without category, while older files still expose category partitions.
                spark_iceberg_multi """
                    alter table demo.${dbName}.${tableName} drop partition field category;
                    delete from demo.${dbName}.${tableName} where id = 8;
                    drop table if exists demo.${dbName}.${tableName}_dimension;
                    create table demo.${dbName}.${tableName}_dimension (category string)
                        using iceberg tblproperties ('format-version'='2');
                    insert into demo.${dbName}.${tableName}_dimension values ('A');
                """
                String finalSnapshot = latestSnapshotId(tableName)
                sql """
                    alter table `${catalogName}`.`${dbName}`.`${tableName}`
                    create tag ${tableName}_final as of version ${finalSnapshot}
                """

                sql """switch ${catalogName}"""
                sql """use ${dbName}"""
                sql """refresh table ${tableName}"""

                // Scenario PE-D05: static partition filters apply every delete exactly once.
                List<List<String>> expectedCurrent = [["1"], ["5"], ["7"]]
                assertEquals(expectedCurrent, stringRows("""
                    select id from ${tableName} where category = 'A' order by id
                """))
                assertEquals([["7", "7000000000"]], stringRows("""
                    select id, payload.metric from ${tableName}
                    where payload.metric > 5000000000 order by id
                """))

                // Scenario PE-D06: runtime filter on the dropped identity partition column returns
                // the same delete-aware rows with partition pruning enabled and disabled.
                String rfQuery = """
                    select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */ f.id
                    from ${tableName} f
                    join ${tableName}_dimension d on f.category = d.category
                    order by f.id
                """
                sql """set runtime_filter_wait_infinitely=true"""
                sql """set disable_join_reorder=true"""
                sql """set enable_runtime_filter_partition_prune=false"""
                assertEquals(expectedCurrent, stringRows(rfQuery))
                sql """set enable_runtime_filter_partition_prune=true"""
                assertEquals(expectedCurrent, stringRows(rfQuery))

                // Scenario PE-D07: numeric snapshots and tags preserve historical rows and deletes.
                assertEquals([["1"], ["2"], ["3"], ["4"]], stringRows("""
                    select id from ${tableName} for version as of ${baseSnapshot}
                    where category in ('A', 'B', 'C') order by id
                """))
                assertEquals([["1"], ["2"]], stringRows("""
                    select id from ${tableName}@tag(${tableName}_base)
                    where category = 'A' order by id
                """))
                assertEquals([["1"]], stringRows("""
                    select id from ${tableName} for version as of ${firstDeleteSnapshot}
                    where category = 'A' order by id
                """))
                assertEquals([["1"], ["5"]], stringRows("""
                    select id from ${tableName} for version as of ${addedDeleteSnapshot}
                    where category = 'A' order by id
                """))
                assertEquals(expectedCurrent, stringRows("""
                    select id from ${tableName}@tag(${tableName}_final)
                    where category = 'A' order by id
                """))

                // Scenario PE-D08: legacy and V2 scanners must agree on multi-spec delete planning.
                sql """set enable_file_scanner_v2=true"""
                List<List<String>> scannerV2Rows = stringRows("""
                    select id from ${tableName} where category = 'A' order by id
                """)
                sql """set enable_file_scanner_v2=false"""
                assertEquals(scannerV2Rows, stringRows("""
                    select id from ${tableName} where category = 'A' order by id
                """))
                sql """set enable_file_scanner_v2=true"""

                // Scenario PE-D09: verify the fixture really contains physical delete artifacts.
                // Iceberg may replace or consolidate older artifacts across delete commits, while
                // the snapshot assertions above still validate every pre/post-evolution phase.
                List<List<String>> deleteFiles = stringRows("""
                    select file_format
                    from ${tableName}\$all_files
                    where content = 1
                    order by file_format
                """)
                assertTrue(deleteFiles.size() >= 1,
                        "${tableName} must contain at least one physical delete artifact")
                if (formatVersion == 3) {
                    assertTrue(deleteFiles.any { row -> row[0].equalsIgnoreCase("PUFFIN") },
                            "${tableName} must contain Iceberg deletion vectors")
                }
            }
        }
    } finally {
        sql """set enable_file_scanner_v2=true"""
        sql """set enable_runtime_filter_partition_prune=true"""
        sql """set disable_join_reorder=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
