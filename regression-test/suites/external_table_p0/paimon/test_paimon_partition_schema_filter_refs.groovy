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

suite("test_paimon_partition_schema_filter_refs",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_partition_schema_filter_refs"
    String dbName = "paimon_partition_schema_filter_refs_db"

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
            def number = matcher.group(1).toString() =~ /([0-9,]+)/
            if (number.find()) {
                values.add(Long.parseLong(number.group(1).replace(",", "")))
            }
        }
        return values
    }
    def assertRuntimeFilterPruned = { String tableName, String dimensionTable,
                                      List<List<String>> expectedRows ->
        String token = UUID.randomUUID().toString()
        assertEquals(expectedRows, stringRows("""
            select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
                '${token}', f.id
            from ${tableName} f join ${dimensionTable} d on f.part = d.part
            order by f.id
        """).collect { row -> [row[1]] })
        // Scanner counters can arrive after the profile list first reports COMPLETE.
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
                "Runtime filter did not prune any Paimon partition/file range; "
                        + profile.take(2000).replaceAll("\\s+", " "))
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
            String tableName = "partition_schema_${format}"
            String dimensionTable = "${tableName}_dimension"
            spark_paimon_multi """
                create database if not exists paimon.${dbName};
                drop table if exists paimon.${dbName}.${tableName};
                create table paimon.${dbName}.${tableName} (
                    id int,
                    part string,
                    payload struct<metric:int, label:string>,
                    attrs map<string, struct<code:int>>,
                    events array<struct<score:int>>
                ) using paimon
                partitioned by (part)
                tblproperties ('file.format'='${format}');
                insert into paimon.${dbName}.${tableName} values
                    (1, 'p1', named_struct('metric', 10, 'label', 'base-1'),
                        map('k', named_struct('code', 100)),
                        array(named_struct('score', 1000))),
                    (2, 'p2', named_struct('metric', 20, 'label', 'base-2'),
                        map('k', named_struct('code', 200)),
                        array(named_struct('score', 2000)));
            """
            String baseSnapshot = latestSnapshotId(tableName)
            createTag(tableName, "${tableName}_base")
            spark_paimon """
                call paimon.sys.create_branch(
                    '${dbName}.${tableName}',
                    '${tableName}_base_branch',
                    '${tableName}_base'
                )
            """

            // Scenario PM-PE01: Paimon keeps a fixed partition key while STRUCT, MAP-value
            // STRUCT and ARRAY-element STRUCT children are added.
            spark_paimon_multi """
                alter table paimon.${dbName}.${tableName} add column payload.extra string;
                alter table paimon.${dbName}.${tableName} add column attrs.value.extra int;
                alter table paimon.${dbName}.${tableName} add column events.element.extra int;
                insert into paimon.${dbName}.${tableName} values
                    (3, 'p1',
                        named_struct('metric', 30, 'label', 'add-3', 'extra', 'payload-extra'),
                        map('k', named_struct('code', 300, 'extra', 301)),
                        array(named_struct('score', 3000, 'extra', 3001))),
                    (4, 'p3',
                        named_struct('metric', 40, 'label', 'add-4', 'extra', 'payload-extra'),
                        map('k', named_struct('code', 400, 'extra', 401)),
                        array(named_struct('score', 4000, 'extra', 4001)));
            """
            String addedSnapshot = latestSnapshotId(tableName)

            // Scenario PM-PE02: rename and promote complex children without changing partition
            // identity. Old snapshot/tag/branch schemas must remain independently readable.
            spark_paimon_multi """
                alter table paimon.${dbName}.${tableName}
                    rename column payload.label to renamed_label;
                alter table paimon.${dbName}.${tableName}
                    rename column attrs.value.code to renamed_code;
                alter table paimon.${dbName}.${tableName}
                    rename column events.element.score to renamed_score;
                alter table paimon.${dbName}.${tableName}
                    alter column payload.metric type bigint;
                insert into paimon.${dbName}.${tableName} values
                    (5, 'p1',
                        named_struct('metric', 5000000000,
                            'renamed_label', 'rename-5', 'extra', 'payload-extra'),
                        map('k', named_struct('renamed_code', 500, 'extra', 501)),
                        array(named_struct('renamed_score', 5000, 'extra', 5001))),
                    (6, 'p4',
                        named_struct('metric', 60,
                            'renamed_label', 'rename-6', 'extra', 'payload-extra'),
                        map('k', named_struct('renamed_code', 600, 'extra', 601)),
                        array(named_struct('renamed_score', 6000, 'extra', 6001)));
            """

            // Scenario PM-PE03: drop/re-add a nested name to verify field IDs never leak values
            // through partition-filtered scans.
            spark_paimon_multi """
                alter table paimon.${dbName}.${tableName} drop column payload.extra;
                alter table paimon.${dbName}.${tableName} add column payload.extra bigint;
                insert into paimon.${dbName}.${tableName} values
                    (7, 'p1',
                        named_struct('metric', 70,
                            'renamed_label', 'readd-7', 'extra', 7000),
                        map('k', named_struct('renamed_code', 700, 'extra', 701)),
                        array(named_struct('renamed_score', 7000, 'extra', 7001)));
                drop table if exists paimon.${dbName}.${dimensionTable};
                create table paimon.${dbName}.${dimensionTable} (part string)
                    using paimon tblproperties ('file.format'='parquet');
                insert into paimon.${dbName}.${dimensionTable} values ('p1');
            """
            String finalSnapshot = latestSnapshotId(tableName)
            createTag(tableName, "${tableName}_final")

            sql """switch ${catalogName}"""
            sql """use ${dbName}"""
            sql """refresh table ${tableName}"""

            // Scenario PM-F01: static partition filtering combines every complex-schema version.
            assertEquals([["1"], ["3"], ["5"], ["7"]], stringRows("""
                select id from ${tableName} where part = 'p1' order by id
            """))
            assertEquals([["5", "5000000000"]], stringRows("""
                select id, payload.metric from ${tableName}
                where part = 'p1' and payload.metric > 1000000000
                order by id
            """))
            assertEquals([["7", "7000"]], stringRows("""
                select id, payload.extra from ${tableName}
                where part = 'p1' and payload.extra is not null
                order by id
            """))

            // Scenario PM-RF01: runtime-filter partition pruning stays result-equivalent across
            // complex schema versions.
            String rfQuery = """
                select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */ f.id
                from ${tableName} f join ${dimensionTable} d on f.part = d.part
                order by f.id
            """
            sql """set runtime_filter_wait_infinitely=true"""
            // Small fixture tables have no column statistics. Keep the generated RF so this
            // suite validates scanner-side partition pruning rather than RF selectivity pruning.
            sql """set enable_runtime_filter_prune=false"""
            sql """set runtime_filter_mode=GLOBAL"""
            sql """set parallel_pipeline_task_num=1"""
            sql """set disable_join_reorder=true"""
            sql """set enable_profile=true"""
            sql """set profile_level=2"""
            sql """set enable_runtime_filter_partition_prune=false"""
            assertEquals([["1"], ["3"], ["5"], ["7"]], stringRows(rfQuery))
            sql """set enable_runtime_filter_partition_prune=true"""
            assertRuntimeFilterPruned(
                    tableName, dimensionTable, [["1"], ["3"], ["5"], ["7"]])

            // Scenario PM-R01: numeric snapshot, tag and branch bind their historical schemas.
            List<List<String>> baseRows = [["1", "p1", "base-1"], ["2", "p2", "base-2"]]
            assertEquals(baseRows, stringRows("""
                select id, part, payload.label
                from ${tableName} for version as of ${baseSnapshot}
                where part in ('p1', 'p2') order by id
            """))
            assertEquals(baseRows, stringRows("""
                select id, part, payload.label
                from ${tableName}@tag(${tableName}_base)
                where part in ('p1', 'p2') order by id
            """))
            assertEquals(baseRows, stringRows("""
                select id, part, payload.label
                from ${tableName}@branch(${tableName}_base_branch)
                where part in ('p1', 'p2') order by id
            """))
            assertEquals([["1"], ["3"]], stringRows("""
                select id from ${tableName} for version as of ${addedSnapshot}
                where part = 'p1' order by id
            """))
            assertEquals([["1"], ["3"], ["5"], ["7"]], stringRows("""
                select id from ${tableName}@tag(${tableName}_final)
                where part = 'p1' order by id
            """))

            // Scenario PM-RD01: JNI and native readers agree for partition-filtered historical
            // and current projections. The assertion is topology-independent.
            sql """set enable_paimon_cpp_reader=false"""
            sql """set force_jni_scanner=true"""
            List<List<String>> jniRows = stringRows("""
                select id, part from ${tableName}
                where part in ('p1', 'p3') order by id
            """)
            sql """set force_jni_scanner=false"""
            sql """set enable_paimon_cpp_reader=true"""
            assertEquals(jniRows, stringRows("""
                select id, part from ${tableName}
                where part in ('p1', 'p3') order by id
            """))
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
