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

// A deletion vector addresses rows by their position in the whole file. Row group and page pruning skip rows before
// the delete predicate runs, so the surviving rows must keep those file positions: if id 101 is deleted and the first
// page was skipped, counting from the first row read would delete id 201 instead. The table has one file whose ids
// equal their positions, which makes such a shift visible in the results.
//
// The profile counters prove that pruning and the deletion vector were both active in the same scan. The abs()
// queries return the same ranges through a predicate that Parquet statistics cannot evaluate, so a pruning-only
// difference shows up between neighbouring result blocks.
suite("test_deltalake_parquet_deletion_vector", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    def profileAction = new ProfileAction(context)
    def counterValues = { String profile, String counter ->
        def values = []
        def matcher = profile =~ ("(?m)^\\s*(?:-\\s*)?"
                + java.util.regex.Pattern.quote(counter) + ":\\s+([^\\n]+)")
        while (matcher.find()) {
            String text = matcher.group(1)
            def exact = text =~ /\(([0-9,]+)\)/
            def number = text =~ /([0-9,]+)/
            values.add(Long.parseLong((exact.find() ? exact.group(1) : number.find() ? number.group(1) : "0")
                    .replace(",", "")))
        }
        return values
    }
    List<String> pageCounters = ["FilteredRowsByPage", "PageIndexReadCalls", "NumDeleteRows"]
    List<String> rangeCounters = pageCounters + ["RowGroupsFilteredByMinMax"]
    sql "set enable_profile=true"
    sql "set enable_sql_cache=false"
    ["filesystem", "hms"].each { String metastore ->
        String catalog = "test_deltalake_parquet_dv_${metastore}"
        String table = "${catalog}.delta_m1.parquet_pruning_dv"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='${metastore}', 'warehouse'='${warehouse}',
            'hive.metastore.uris'='thrift://${host}:${hmsPort}',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""

        def checkPrunedRows = { String name, String predicate, List<String> counters ->
            String token = UUID.randomUUID().toString()
            "order_qt_${metastore}_${name}"("""
                select /* ${token} */ id, payload from ${table} where ${predicate} order by id
            """)
            String profile = profileAction.getProfileBySql(token,
                    counters + ["Delta Scan Metrics", "EnumerationComplete: true"])
            assertEquals([1L], counterValues(profile, "SelectedFiles"), profile)
            counters.each { String counter ->
                assertTrue(counterValues(profile, counter).any { it > 0 },
                        "${metastore}/${name}: expected positive ${counter}; ${profile}")
            }
            return profile
        }

        // First row group, around the start of the second page of the id column, near the page edge at about id 200,
        // and a row group far from the start of the file.
        Map<String, List<Integer>> ranges = ["early": [100, 120], "page_edge": [195, 205], "late": [9000, 9020]]
        ranges.each { String range, List<Integer> bounds ->
            int lower = bounds[0]
            int upper = bounds[1]
            String profile = checkPrunedRows(range, "id >= ${lower} and id < ${upper}", rangeCounters)
            assertEquals([1L], counterValues(profile, "GeneratedSplits").unique(), profile)
            "order_qt_${metastore}_${range}_no_pruning"("""
                select id, payload
                from ${table} where abs(id) >= ${lower} and abs(id) < ${upper} order by id
            """)
            "qt_${metastore}_${range}_count"(
                    "select count(*) from ${table} where id >= ${lower} and id < ${upper}")
        }

        // On the first selected page only id 5 matches, and it is deleted. Continue past the gap for 201 and 204.
        String disjoint = checkPrunedRows("disjoint_pages", "id in (5, 200, 201, 204)",
                rangeCounters + ["SelectedRowRanges", "RowGroupsReadNum"])
        assertEquals(1L, counterValues(disjoint, "RowGroupsReadNum").max(), disjoint)
        assertTrue(counterValues(disjoint, "SelectedRowRanges").max() >= 2, disjoint)

        String deleted = "id >= 199 and id < 201"
        checkPrunedRows("all_deleted", deleted, rangeCounters)
        "qt_${metastore}_all_deleted_aggregates"(
                "select count(*), min(id), max(id) from ${table} where ${deleted}")

        def previousSplitSize = sql("select @@file_split_size")[0][0]
        try {
            sql "set file_split_size=32768"
            String predicate = "id in (101, 102, 9005, 9006)"
            // Deleted positions 101 and 9005 still refer to the whole file when its later split starts.
            String splitProfile = checkPrunedRows("cross_split", predicate, pageCounters + ["RowGroupsReadNum"])
            assertTrue(counterValues(splitProfile, "GeneratedSplits").min() > 1, splitProfile)
            assertTrue(counterValues(splitProfile, "RowGroupsReadNum").max() >= 2, splitProfile)
        } finally {
            sql "set file_split_size=${previousSplitSize}"
        }

        // Footer statistics still count deleted rows. Ids 0 and 9999 are deleted, so MIN and MAX must not use them.
        "qt_${metastore}_count"("select count(*) from ${table}")
        "qt_${metastore}_min_max"("select min(id), max(id) from ${table}")
        "qt_${metastore}_misaligned_payloads"("""
            select count(*) from ${table} where not (payload <=> rpad(concat('row-', cast(id as string)), 64, 'x'))
        """)
    }
}
