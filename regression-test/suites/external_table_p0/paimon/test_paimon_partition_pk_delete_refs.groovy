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

suite("test_paimon_partition_pk_delete_refs",
        "p0,external,paimon,external_docker,external_docker_paimon,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_partition_pk_delete_refs"
    String dbName = "paimon_partition_pk_delete_refs_db"

    def stringRows = { String query ->
        sql(query).collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }
    def latestSnapshotId = { String tableName ->
        return spark_paimon("""
            select snapshot_id
            from paimon.${dbName}.`${tableName}\$snapshots`
            order by snapshot_id desc
            limit 1
        """)[0][0].toString()
    }
    def createTag = { String tableName, String tagName ->
        spark_paimon """
            call paimon.sys.create_tag(
                table => '${dbName}.${tableName}',
                tag => '${tagName}'
            )
        """
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
                '${token}', f.id, f.full_name
            from ${tableName} f
            join ${dimensionTable} d on f.part = d.part
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
                "Runtime filter did not prune a Paimon PK partition/file range; "
                        + profile.take(2000).replaceAll("\\s+", " "))
        return rows.collect { row -> [row[1], row[2]] }
    }
    def getExplainText = { String query ->
        return sql("explain verbose ${query}").collect { row -> row[0].toString() }.join("\n")
    }
    def assertNativePath = { String query, String label ->
        String explainText = getExplainText(query)
        def splitMatcher = (explainText =~ /paimonNativeReadSplits=(\d+)\/(\d+)/)
        assertTrue(splitMatcher.find(), "Expected paimonNativeReadSplits for ${label}")
        long nativeSplits = Long.parseLong(splitMatcher.group(1))
        long totalSplits = Long.parseLong(splitMatcher.group(2))
        assertTrue(totalSplits > 0 && nativeSplits > 0,
                "Expected native splits for ${label}, native=${nativeSplits}, total=${totalSplits}")
        assertTrue(explainText.contains("SplitStat [type=NATIVE"),
                "Expected a NATIVE split for ${label}")
    }
    def assertJniPath = { String query, String label ->
        String explainText = getExplainText(query)
        def splitMatcher = (explainText =~ /paimonNativeReadSplits=(\d+)\/(\d+)/)
        assertTrue(splitMatcher.find(), "Expected paimonNativeReadSplits for ${label}")
        long nativeSplits = Long.parseLong(splitMatcher.group(1))
        long totalSplits = Long.parseLong(splitMatcher.group(2))
        assertTrue(totalSplits > 0 && nativeSplits == 0,
                "Expected JNI-only splits for ${label}, native=${nativeSplits}, total=${totalSplits}")
        assertTrue(explainText.contains("SplitStat [type=JNI"),
                "Expected a JNI split for ${label}")
    }

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
        ["parquet", "orc"].each { String format ->
            String tableName = "partition_pk_dv_${format}"
            String dimensionTable = "${tableName}_dimension"
            spark_paimon_multi """
                create database if not exists paimon.${dbName};
                drop table if exists paimon.${dbName}.${tableName};
                create table paimon.${dbName}.${tableName} (
                    id int not null,
                    part string not null,
                    old_name string,
                    note string,
                    payload struct<metric:int, label:string>
                ) using paimon
                partitioned by (part)
                tblproperties (
                    'bucket'='1',
                    'primary-key'='part,id',
                    'file.format'='${format}',
                    'deletion-vectors.enabled'='true'
                );
                insert into paimon.${dbName}.${tableName} values
                    (1, 'p1', 'alpha', 'old-note-1',
                        named_struct('metric', 10, 'label', 'base-1')),
                    (2, 'p1', 'beta', 'old-note-2',
                        named_struct('metric', 20, 'label', 'base-delete')),
                    (3, 'p2', 'gamma', 'old-note-3',
                        named_struct('metric', 30, 'label', 'base-p2')),
                    (90, 'p_dv', 'dv-victim', 'old-note-90',
                        named_struct('metric', 90, 'label', 'dv-victim'));
            """
            String baseSnapshot = latestSnapshotId(tableName)
            createTag(tableName, "${tableName}_base")

            // Scenario PM-D01: add/rename fields, upsert one PK and delete another inside p1.
            spark_paimon_multi """
                alter table paimon.${dbName}.${tableName} add column payload.extra string;
                alter table paimon.${dbName}.${tableName} rename column old_name to full_name;
                insert into paimon.${dbName}.${tableName}
                    (id, part, full_name, note, payload) values
                    (1, 'p1', 'alpha-updated', 'new-note-1',
                        named_struct('metric', 11, 'label', 'updated-1', 'extra', 'extra-1')),
                    (4, 'p1', 'delta', 'delete-later',
                        named_struct('metric', 40, 'label', 'insert-4', 'extra', 'extra-4'));
                delete from paimon.${dbName}.${tableName}
                    where part = 'p1' and id = 2;
            """
            String firstDeleteSnapshot = latestSnapshotId(tableName)
            createTag(tableName, "${tableName}_first_delete")

            // Scenario PM-D02: nested rename/type promotion and drop/re-add combine with another
            // delete, insert and full compaction while the partition key stays fixed.
            spark_paimon_multi """
                alter table paimon.${dbName}.${tableName}
                    rename column payload.label to renamed_label;
                alter table paimon.${dbName}.${tableName}
                    alter column payload.metric type bigint;
                alter table paimon.${dbName}.${tableName} drop column note;
                alter table paimon.${dbName}.${tableName} add column note bigint;
                delete from paimon.${dbName}.${tableName}
                    where part = 'p1' and id = 4;
                insert into paimon.${dbName}.${tableName}
                    (id, part, full_name, payload, note) values
                    (5, 'p1', 'epsilon',
                        named_struct('metric', 5000000000,
                            'renamed_label', 'insert-5', 'extra', 'extra-5'),
                        5000);
                call paimon.sys.compact(
                    table => '${dbName}.${tableName}',
                    compact_strategy => 'full'
                );
                call paimon.sys.create_tag(
                    table => '${dbName}.${tableName}',
                    tag => '${tableName}_pre_dv'
                );
                -- Prevent synchronous batch compaction from materializing the sacrificial
                -- delete, so the fixture retains a physical DV artifact for reader checks.
                alter table paimon.${dbName}.${tableName}
                    set tblproperties ('write-only'='true');
                delete from paimon.${dbName}.${tableName}
                    where part = 'p_dv' and id = 90;
                drop table if exists paimon.${dbName}.${dimensionTable};
                create table paimon.${dbName}.${dimensionTable} (part string)
                    using paimon tblproperties ('file.format'='parquet');
                insert into paimon.${dbName}.${dimensionTable} values ('p1');
            """
            long deletedRowsInFiles = spark_paimon("""
                select coalesce(sum(deleteRowCount), 0)
                from paimon.${dbName}.`${tableName}\$files`
            """)[0][0].toString().toLong()
            assertTrue(deletedRowsInFiles > 0,
                    "${tableName} must retain a physical deletion-vector row")
            String finalSnapshot = latestSnapshotId(tableName)
            createTag(tableName, "${tableName}_final")

            sql """switch ${catalogName}"""
            sql """use ${dbName}"""
            sql """refresh table ${tableName}"""
            String actionSuffix = format

            // Scenario PM-D03: static partition filters apply PK upserts, deletes and DV state.
            List<List<String>> expectedCurrent = [["1", "alpha-updated"], ["5", "epsilon"]]
            "qt_${actionSuffix}_current_partition_filter"("""
                select id, full_name from ${tableName}
                where part = 'p1' order by id
            """)
            "qt_${actionSuffix}_current_promoted_and_readded"("""
                select id, payload.metric, note from ${tableName}
                where part = 'p1' and payload.metric > 1000000000
                order by id
            """)
            // Surviving pre-readd row 1 must see NULL for the new BIGINT note field, while the
            // nested rename and added child stay bound by field ID across the delete timeline.
            "qt_${actionSuffix}_current_evolved_fields"("""
                select id, payload.renamed_label, payload.extra, note
                from ${tableName}
                where part = 'p1'
                order by id
            """)
            "qt_${actionSuffix}_pre_dv_old_row_null"("""
                select id, payload.renamed_label, payload.extra, note
                from ${tableName}@tag(${tableName}_pre_dv)
                where part = 'p1' and id = 1
            """)
            test {
                sql """select payload.label from ${tableName} where part = 'p1'"""
                exception "label"
            }

            // Scenario PM-D04: runtime-filter pruning on the partition key remains delete-aware.
            String rfQuery = """
                select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
                    f.id, f.full_name
                from ${tableName} f
                join ${dimensionTable} d on f.part = d.part
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
            assertEquals(expectedCurrent, assertRuntimeFilterPruned(tableName, dimensionTable))

            // Scenario PM-D05: numeric snapshots and tags preserve the matching schema/delete set.
            "qt_${actionSuffix}_base_snapshot"("""
                select id, old_name, note, payload.label
                from ${tableName} for version as of ${baseSnapshot}
                where part = 'p1' order by id
            """)
            "qt_${actionSuffix}_base_tag"("""
                select id, old_name, note, payload.label
                from ${tableName}@tag(${tableName}_base)
                where part = 'p1' order by id
            """)
            "qt_${actionSuffix}_first_delete_snapshot"("""
                select id, full_name, note, payload.label, payload.extra
                from ${tableName} for version as of ${firstDeleteSnapshot}
                where part = 'p1' order by id
            """)
            "qt_${actionSuffix}_final_tag"("""
                select id, full_name, note, payload.renamed_label, payload.extra
                from ${tableName}@tag(${tableName}_final)
                where part = 'p1' order by id
            """)
            test {
                sql """
                    select payload.renamed_label
                    from ${tableName} for version as of ${baseSnapshot}
                """
                exception "renamed_label"
            }

            // Scenario PM-D06: JNI/native readers agree for current DV state and historical
            // projections, and explain must prove the requested reader path rather than fallback.
            String currentReaderQuery = """
                select id, part, full_name, note, payload.renamed_label, payload.extra
                from ${tableName}
                where part in ('p1', 'p2')
                order by part, id
            """
            String historicalReaderQuery = """
                select id, part, old_name, note, payload.label
                from ${tableName} for version as of ${baseSnapshot}
                where part in ('p1', 'p2')
                order by part, id
            """
            sql """set enable_paimon_cpp_reader=false"""
            sql """set force_jni_scanner=true"""
            assertJniPath(currentReaderQuery, "${tableName} current DV")
            assertJniPath(historicalReaderQuery, "${tableName} historical")
            "qt_${actionSuffix}_jni_current_dv"(currentReaderQuery)
            "qt_${actionSuffix}_jni_historical"(historicalReaderQuery)
            sql """set force_jni_scanner=false"""
            sql """set enable_paimon_cpp_reader=true"""
            assertNativePath(currentReaderQuery, "${tableName} current DV")
            assertNativePath(historicalReaderQuery, "${tableName} historical")
            "qt_${actionSuffix}_native_current_dv"(currentReaderQuery)
            "qt_${actionSuffix}_native_historical"(historicalReaderQuery)
            assertEquals(finalSnapshot, latestSnapshotId(tableName))
        }
    } finally {
        sql """set enable_paimon_cpp_reader=false"""
        sql """set force_jni_scanner=false"""
        sql """set enable_runtime_filter_prune=true"""
        sql """set enable_runtime_filter_partition_prune=true"""
        sql """set disable_join_reorder=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
