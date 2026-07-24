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

suite("test_iceberg_partition_evolution_runtime_filter",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_partition_evolution_runtime_filter"
    String dbName = "iceberg_partition_evolution_runtime_filter_db"
    String factTable = "evolved_fact"
    String dimensionTable = "rf_dimension"

    def stringRows = { String query ->
        sql(query).collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }
    def latestSnapshotId = {
        return spark_iceberg("""
            select snapshot_id
            from demo.${dbName}.${factTable}.snapshots
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
    def assertRuntimeFilterPruned = { String queryBody ->
        String token = UUID.randomUUID().toString()
        List<List<String>> rows = stringRows("""
            select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
                '${token}', f.id
            ${queryBody}
            order by f.id
        """)
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
                "Runtime filter did not prune any evolved Iceberg partition/file range; "
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
        spark_iceberg_multi """
            create database if not exists demo.${dbName};
            drop table if exists demo.${dbName}.${factTable};
            create table demo.${dbName}.${factTable} (
                id int,
                category string,
                event_time timestamp,
                payload struct<metric:int>
            ) using iceberg
            partitioned by (category, days(event_time))
            tblproperties ('format-version'='2');
            insert into demo.${dbName}.${factTable} values
                (1, 'A', timestamp '2026-01-01 01:00:00', named_struct('metric', 10)),
                (2, 'B', timestamp '2026-01-02 01:00:00', named_struct('metric', 20)),
                (3, 'C', timestamp '2026-01-03 01:00:00', named_struct('metric', 30));
        """
        String baseSnapshot = latestSnapshotId()
        sql """
            alter table `${catalogName}`.`${dbName}`.`${factTable}`
            create tag rf_base as of version ${baseSnapshot}
        """

        // Scenario PE-RF01: add a partition field and evolve a nested payload between data files.
        spark_iceberg_multi """
            alter table demo.${dbName}.${factTable} add partition field bucket(8, id);
            alter table demo.${dbName}.${factTable} add column payload.label string;
            insert into demo.${dbName}.${factTable} values
                (4, 'A', timestamp '2026-02-01 01:00:00',
                    named_struct('metric', 40, 'label', 'add-spec')),
                (5, 'D', timestamp '2026-02-02 01:00:00',
                    named_struct('metric', 50, 'label', 'add-spec'));
        """
        String addedSnapshot = latestSnapshotId()

        // Scenario PE-RF02: replace a temporal transform. Runtime filters on event_time must
        // translate against the transform of each file's own spec.
        spark_iceberg_multi """
            alter table demo.${dbName}.${factTable}
                replace partition field days(event_time) with months(event_time);
            insert into demo.${dbName}.${factTable} values
                (6, 'A', timestamp '2026-03-01 01:00:00',
                    named_struct('metric', 60, 'label', 'replace-spec')),
                (7, 'E', timestamp '2026-03-02 01:00:00',
                    named_struct('metric', 70, 'label', 'replace-spec'));
        """

        // Scenario PE-RF03: drop the identity field. New files have no category partition value;
        // runtime pruning may only discard older ranges and must still scan matching new rows.
        spark_iceberg_multi """
            alter table demo.${dbName}.${factTable} drop partition field category;
            insert into demo.${dbName}.${factTable} values
                (8, 'A', timestamp '2026-04-01 01:00:00',
                    named_struct('metric', 80, 'label', 'drop-spec')),
                (9, 'F', timestamp '2026-04-02 01:00:00',
                    named_struct('metric', 90, 'label', 'drop-spec'));
            drop table if exists demo.${dbName}.${dimensionTable};
            create table demo.${dbName}.${dimensionTable} (
                category string,
                lower_time timestamp,
                upper_time timestamp
            ) using iceberg
            tblproperties ('format-version'='2');
            insert into demo.${dbName}.${dimensionTable} values
                ('A', timestamp '2026-03-01 00:00:00', timestamp '2026-05-01 00:00:00');
        """
        String droppedSnapshot = latestSnapshotId()
        sql """
            alter table `${catalogName}`.`${dbName}`.`${factTable}`
            create tag rf_dropped as of version ${droppedSnapshot}
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh catalog ${catalogName}"""
        sql """set enable_profile=true"""
        sql """set profile_level=2"""
        // Small fixture tables have no column statistics. Keep the generated RF so this suite
        // validates scanner-side partition pruning instead of optimizer selectivity heuristics.
        sql """set enable_runtime_filter_prune=false"""
        sql """set runtime_filter_wait_infinitely=true"""
        sql """set runtime_filter_mode=GLOBAL"""
        sql """set parallel_pipeline_task_num=1"""
        sql """set disable_join_reorder=true"""

        String currentCategoryJoin = """
            from ${factTable} f
            join ${dimensionTable} d on f.category = d.category
        """
        String currentTemporalJoin = """
            from ${factTable} f
            join ${dimensionTable} d
              on f.event_time >= d.lower_time and f.event_time < d.upper_time
        """

        // Scenario PE-RF04: result parity with RF disabled protects correctness.
        sql """set enable_runtime_filter_partition_prune=false"""
        assertEquals([["1"], ["4"], ["6"], ["8"]], stringRows("""
            select f.id ${currentCategoryJoin} order by f.id
        """))
        assertEquals([["6"], ["7"], ["8"], ["9"]], stringRows("""
            select f.id ${currentTemporalJoin} order by f.id
        """))

        // Scenario PE-RF05: current multi-spec scan must both return the same rows and show
        // physical pruning in the profile.
        sql """set enable_runtime_filter_partition_prune=true"""
        assertEquals([["1"], ["4"], ["6"], ["8"]],
                assertRuntimeFilterPruned(currentCategoryJoin))

        // Scenario PE-RF06: numeric snapshot and tag retain runtime-filter correctness.
        assertEquals([["1"]], stringRows("""
            select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */ f.id
            from ${factTable} for version as of ${baseSnapshot} f
            join ${dimensionTable} d on f.category = d.category
            order by f.id
        """))
        assertEquals([["1"], ["4"]], stringRows("""
            select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */ f.id
            from ${factTable} for version as of ${addedSnapshot} f
            join ${dimensionTable} d on f.category = d.category
            order by f.id
        """))
        assertEquals([["1"], ["4"], ["6"], ["8"]], stringRows("""
            select /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */ f.id
            from ${factTable}@tag(rf_dropped) f
            join ${dimensionTable} d on f.category = d.category
            order by f.id
        """))
    } finally {
        sql """set enable_runtime_filter_prune=true"""
        sql """set enable_runtime_filter_partition_prune=true"""
        sql """set disable_join_reorder=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
