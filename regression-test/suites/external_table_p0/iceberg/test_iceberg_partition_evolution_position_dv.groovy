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

import org.apache.doris.regression.action.ProfileAction

suite("test_iceberg_partition_evolution_position_dv",
        "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
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
    def profileAction = new ProfileAction(context)
    def profileCounterValues = { String profileText, String counterName ->
        def values = []
        def matcher = profileText =~ ("(?m)^\\s*(?:-\\s*)?"
                + java.util.regex.Pattern.quote(counterName) + ":\\s+([^\\n]+)")
        while (matcher.find()) {
            String valueText = matcher.group(1).toString()
            def exact = valueText =~ /\(([0-9,]+)\)/
            def number = valueText =~ /([0-9,]+)/
            String rawValue = exact.find() ? exact.group(1) : (number.find() ? number.group(1) : null)
            if (rawValue != null) {
                values.add(Long.parseLong(rawValue.replace(",", "")))
            }
        }
        return values
    }
    def assertRuntimeFilterPruned = { String tableName, String dimensionTable ->
        String token = UUID.randomUUID().toString()
        List<List<String>> rows = stringRows("""
            select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
                '${token}', f.id
            from ${tableName} f
            join ${dimensionTable} d on f.category = d.category
            order by f.id
        """)
        String profile = profileAction.getProfileBySql(
                token,
                ["RuntimeFilterPartitionPrunedRangeNum"],
                30000L,
                500L)
        long fileRangesPruned = profileCounterValues(
                profile, "RuntimeFilterPartitionPrunedRangeNum").sum(0L)
        long partitionsPruned = profileCounterValues(
                profile, "PartitionsPrunedByRuntimeFilter").sum(0L)
        assertTrue(fileRangesPruned + partitionsPruned > 0L,
                "Runtime filter did not prune a delete-aware Iceberg partition/file range; "
                        + profile.take(2000).replaceAll("\\s+", " "))
        return rows.collect { row -> [row[1]] }
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
                        code string,
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
                    select /*+ coalesce(1) */ id, category, code, event_time, payload from values
                        (1, 'A', 'aa-1', timestamp '2026-01-01 01:00:00',
                            named_struct('metric', 10, 'label', 'base-a')),
                        (2, 'A', 'aa-2', timestamp '2026-01-01 02:00:00',
                            named_struct('metric', 20, 'label', 'base-delete')),
                        (3, 'B', 'bb-1', timestamp '2026-01-02 01:00:00',
                            named_struct('metric', 30, 'label', 'base-b')),
                        (4, 'C', 'cc-1', timestamp '2026-01-03 01:00:00',
                            named_struct('metric', 40, 'label', 'base-c'))
                        as t(id, category, code, event_time, payload);
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
                    select /*+ coalesce(1) */ id, category, code, event_time, payload from values
                        (5, 'A', 'aa-3', timestamp '2026-02-01 01:00:00',
                            named_struct('metric', 50, 'label', 'add-a', 'extra', 'new-child')),
                        (6, 'A', 'aa-4', timestamp '2026-02-01 02:00:00',
                            named_struct('metric', 60, 'label', 'add-delete', 'extra', 'new-child'))
                        as t(id, category, code, event_time, payload);
                    delete from demo.${dbName}.${tableName} where id = 6;
                """
                String addedDeleteSnapshot = latestSnapshotId(tableName)

                // Scenario PE-D03: REPLACE temporal transform and rename/promote nested fields.
                spark_iceberg_multi """
                    alter table demo.${dbName}.${tableName}
                        replace partition field days(event_time) with months(event_time);
                    alter table demo.${dbName}.${tableName}
                        replace partition field bucket(8, id) with truncate(2, code);
                    alter table demo.${dbName}.${tableName}
                        rename column payload.label to renamed_label;
                    alter table demo.${dbName}.${tableName}
                        alter column payload.metric type bigint;
                    insert into demo.${dbName}.${tableName} values
                        (7, 'A', 'aa-5', timestamp '2026-03-01 01:00:00',
                            named_struct('metric', 7000000000,
                                'renamed_label', 'replace-a', 'extra', 'renamed-child')),
                        (8, 'A', 'aa-6', timestamp '2026-03-01 02:00:00',
                            named_struct('metric', 80,
                                'renamed_label', 'replace-delete', 'extra', 'renamed-child'));
                """

                // Scenario PE-D04: DROP identity field, write a victim with the resulting spec,
                // then delete that victim. The data and delete artifacts must both use the spec
                // that lacks category while older files still expose category partitions.
                spark_iceberg_multi """
                    alter table demo.${dbName}.${tableName} drop partition field category;
                    delete from demo.${dbName}.${tableName} where id = 8;
                    insert into demo.${dbName}.${tableName}
                    select /*+ coalesce(1) */ id, category, code, event_time, payload from values
                        (9, 'A', 'aa-7', timestamp '2026-04-01 01:00:00',
                            named_struct('metric', 90,
                                'renamed_label', 'drop-delete', 'extra', 'new-spec')),
                        (10, 'A', 'aa-8', timestamp '2026-04-01 02:00:00',
                            named_struct('metric', 100,
                                'renamed_label', 'drop-survivor', 'extra', 'new-spec'))
                        as t(id, category, code, event_time, payload);
                    delete from demo.${dbName}.${tableName} where id = 9;
                    drop table if exists demo.${dbName}.${tableName}_dimension;
                    create table demo.${dbName}.${tableName}_dimension (category string)
                        using iceberg tblproperties ('format-version'='2');
                    insert into demo.${dbName}.${tableName}_dimension values ('A');
                """
                List<List<Object>> dataSpecRows = spark_iceberg """
                    select max(spec_id)
                    from demo.${dbName}.${tableName}.all_files
                    where content = 0
                """
                List<List<Object>> deleteSpecRows = spark_iceberg """
                    select max(spec_id)
                    from demo.${dbName}.${tableName}.all_files
                    where content = 1
                """
                assertEquals(dataSpecRows[0][0], deleteSpecRows[0][0],
                        "The post-drop victim data and delete artifacts must use the same spec")
                String finalSnapshot = latestSnapshotId(tableName)
                sql """
                    alter table `${catalogName}`.`${dbName}`.`${tableName}`
                    create tag ${tableName}_final as of version ${finalSnapshot}
                """

                sql """switch ${catalogName}"""
                sql """use ${dbName}"""
                sql """refresh table ${tableName}"""
                String actionSuffix = "${deleteKind}_${format}"

                // Scenario PE-D05: static partition filters apply every delete exactly once.
                List<List<String>> expectedCurrent = [["1"], ["5"], ["7"], ["10"]]
                "qt_${actionSuffix}_current_filter"("""
                    select id from ${tableName} where category = 'A' order by id
                """)
                "qt_${actionSuffix}_current_nested"("""
                    select id, payload.metric from ${tableName}
                    where payload.metric > 5000000000 order by id
                """)

                // Scenario PE-D06: runtime filter on the dropped identity partition column returns
                // the same delete-aware rows with pruning disabled, then proves that the enabled
                // query retains an RF and prunes at least one partition/file range.
                String rfQuery = """
                    select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */ f.id
                    from ${tableName} f
                    join ${tableName}_dimension d on f.category = d.category
                    order by f.id
                """
                sql """set runtime_filter_wait_infinitely=true"""
                sql """set disable_join_reorder=true"""
                sql """set enable_runtime_filter_prune=false"""
                sql """set runtime_filter_mode=GLOBAL"""
                sql """set parallel_pipeline_task_num=1"""
                sql """set enable_profile=true"""
                sql """set profile_level=2"""
                sql """set enable_runtime_filter_partition_prune=false"""
                "qt_${actionSuffix}_rf_disabled"(rfQuery)
                sql """set enable_runtime_filter_partition_prune=true"""
                assertEquals(expectedCurrent,
                        assertRuntimeFilterPruned(tableName, "${tableName}_dimension"))

                // Scenario PE-D07: numeric snapshots and tags preserve historical rows and deletes.
                "qt_${actionSuffix}_base_snapshot"("""
                    select id from ${tableName} for version as of ${baseSnapshot}
                    where category in ('A', 'B', 'C') order by id
                """)
                "qt_${actionSuffix}_base_tag"("""
                    select id from ${tableName}@tag(${tableName}_base)
                    where category = 'A' order by id
                """)
                "qt_${actionSuffix}_first_delete_snapshot"("""
                    select id from ${tableName} for version as of ${firstDeleteSnapshot}
                    where category = 'A' order by id
                """)
                "qt_${actionSuffix}_added_delete_snapshot"("""
                    select id from ${tableName} for version as of ${addedDeleteSnapshot}
                    where category = 'A' order by id
                """)
                "qt_${actionSuffix}_final_tag"("""
                    select id from ${tableName}@tag(${tableName}_final)
                    where category = 'A' order by id
                """)

                // Scenario PE-D08: legacy and V2 scanners must agree on multi-spec delete planning.
                sql """set enable_file_scanner_v2=true"""
                "qt_${actionSuffix}_scanner_v2"("""
                    select id from ${tableName} where category = 'A' order by id
                """)
                sql """set enable_file_scanner_v2=false"""
                "qt_${actionSuffix}_scanner_v1"("""
                    select id from ${tableName} where category = 'A' order by id
                """)
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
        sql """set enable_runtime_filter_prune=true"""
        sql """set enable_runtime_filter_partition_prune=true"""
        sql """set disable_join_reorder=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
