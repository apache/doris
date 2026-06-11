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

suite("rf_partition_pruning_type_matrix", "nonConcurrent") {
    sql "set enable_runtime_filter_prune=false"
    sql "set enable_runtime_filter_partition_prune=true"
    sql "set runtime_filter_wait_infinitely=true"
    sql "set disable_join_reorder=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"
    sql "set parallel_pipeline_task_num=1"
    // Keep IN_OR_BLOOM_FILTER on the IN path so pruning counters are deterministic.
    sql "set runtime_filter_max_in_num=1024"

    def profileAction = new ProfileAction(context)
    def rfPruningCounterNames = ["TotalPartitionsForRFPruning", "PartitionsPrunedByRuntimeFilter"]
    def profileCompletionStateName = "Profile Completion State"
    def profileCompletionStateComplete = "COMPLETE"

    def getProfileByToken = { String token ->
        String profileContent = ""
        String profileState = ""
        for (int attempt = 0; attempt < 60; attempt++) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileState = profileItem[profileCompletionStateName]?.toString()
                    if (profileState == profileCompletionStateComplete) {
                        profileContent = profileAction.getProfile(profileItem["Profile ID"].toString())
                    }
                    break
                }
            }
            if (profileState == profileCompletionStateComplete && profileContent != ""
                    && rfPruningCounterNames.every { profileContent.contains(it) }) {
                break
            }
            Thread.sleep(500)
        }
        return profileContent
    }

    def extractCounterSum = { String profileText, String counterName ->
        def values = (profileText =~ /-\s*${counterName}:\s*(\d+)/).collect { it[1].toLong() }
        return values.isEmpty() ? 0L : values.sum()
    }

    def assertPruningProfile = { String queryBody, String rfType, long expectedTotal, long expectedPruned ->
        def token = UUID.randomUUID().toString()
        def querySql = """
            SELECT /*+ SET_VAR(runtime_filter_type='${rfType}') */ "${token}", *
            ${queryBody}
            ORDER BY 2
        """
        sql querySql
        def profile = getProfileByToken(token)
        assertTrue(profile != "", "Profile not found for ${token}")
        def missingCounters = rfPruningCounterNames.findAll { !profile.contains(it) }
        assertTrue(missingCounters.isEmpty(),
                "Profile missing RF pruning counters ${missingCounters} for ${token}; "
                        + profile.take(1000).replaceAll("\\s+", " "))
        def total = extractCounterSum(profile, "TotalPartitionsForRFPruning")
        def pruned = extractCounterSum(profile, "PartitionsPrunedByRuntimeFilter")
        logger.info("RF type matrix profile [${token}]: total=${total}, pruned=${pruned}")
        assertTrue(total >= expectedTotal,
                "TotalPartitionsForRFPruning: expected >= ${expectedTotal}, got ${total}")
        assertTrue(pruned >= expectedPruned,
                "PartitionsPrunedByRuntimeFilter: expected >= ${expectedPruned}, got ${pruned}")
    }

    def runRangeCase = { String suffix, String partType, List<String> bounds, String dimVal,
                         List<String> rowVals ->
        def fact = "rf_prune_type_matrix_range_${suffix}"
        def dim = "rf_prune_type_matrix_range_dim_${suffix}"
        sql "drop table if exists ${fact}"
        sql """
            CREATE TABLE ${fact} (
                id INT NOT NULL,
                part_col ${partType} NOT NULL,
                value VARCHAR(32)
            )
            PARTITION BY RANGE(part_col) (
                PARTITION p1 VALUES [(${bounds[0]}), (${bounds[1]})),
                PARTITION p2 VALUES [(${bounds[1]}), (${bounds[2]})),
                PARTITION p3 VALUES [(${bounds[2]}), (${bounds[3]})),
                PARTITION p4 VALUES [(${bounds[3]}), (${bounds[4]}))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES("replication_num" = "1")
        """
        def rows = rowVals.withIndex().collect { v, i -> "(${i + 1}, ${v}, 'r${i}')" }.join(", ")
        sql "insert into ${fact} values ${rows}"

        sql "drop table if exists ${dim}"
        sql """
            CREATE TABLE ${dim} (
                id INT NOT NULL,
                dim_key ${partType} NOT NULL
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql "insert into ${dim} values (1, ${dimVal})"

        def queryBody = "FROM ${fact} f JOIN ${dim} d ON f.part_col = d.dim_key"
        assertPruningProfile(queryBody, "IN_OR_BLOOM_FILTER", 4, 3)
        assertPruningProfile(queryBody, "MIN_MAX", 4, 3)
    }

    def runListCase = { String suffix, String partType, List<String> values, String dimVal,
                        boolean testMinMax = true ->
        def fact = "rf_prune_type_matrix_list_${suffix}"
        def dim = "rf_prune_type_matrix_list_dim_${suffix}"
        sql "drop table if exists ${fact}"
        sql """
            CREATE TABLE ${fact} (
                id INT NOT NULL,
                part_col ${partType} NOT NULL,
                value VARCHAR(32)
            )
            PARTITION BY LIST(part_col) (
                ${values.withIndex().collect { v, i -> "PARTITION p${i + 1} VALUES IN (${v})" }.join(",\n                ")}
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES("replication_num" = "1")
        """
        def rows = values.withIndex().collect { v, i -> "(${i + 1}, ${v}, 'r${i}')" }.join(", ")
        sql "insert into ${fact} values ${rows}"

        sql "drop table if exists ${dim}"
        sql """
            CREATE TABLE ${dim} (
                id INT NOT NULL,
                dim_key ${partType} NOT NULL
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql "insert into ${dim} values (1, ${dimVal})"

        def queryBody = "FROM ${fact} f JOIN ${dim} d ON f.part_col = d.dim_key"
        assertPruningProfile(queryBody, "IN_OR_BLOOM_FILTER", values.size(), values.size() - 1)
        if (testMinMax) {
            assertPruningProfile(queryBody, "MIN_MAX", values.size(), values.size() - 1)
        }
    }

    runRangeCase("tinyint", "TINYINT",
            ["-100", "-50", "0", "50", "100"], "-25",
            ["-75", "-25", "25", "75"])
    runRangeCase("smallint", "SMALLINT",
            ["0", "1000", "2000", "3000", "4000"], "1500",
            ["100", "1500", "2500", "3500"])
    runRangeCase("int", "INT",
            ["0", "100", "200", "300", "400"], "150",
            ["50", "150", "250", "350"])
    runRangeCase("bigint", "BIGINT",
            ["0", "100000", "200000", "300000", "400000"], "150000",
            ["50000", "150000", "250000", "350000"])
    runRangeCase("largeint", "LARGEINT",
            ["0", "100000000000000000000", "200000000000000000000",
             "300000000000000000000", "400000000000000000000"],
            "150000000000000000000",
            ["50000000000000000000", "150000000000000000000",
             "250000000000000000000", "350000000000000000000"])
    runRangeCase("date", "DATE",
            ["'2024-01-01'", "'2024-04-01'", "'2024-07-01'", "'2024-10-01'", "'2025-01-01'"],
            "'2024-05-15'",
            ["'2024-02-15'", "'2024-05-15'", "'2024-08-15'", "'2024-11-15'"])
    runRangeCase("datetime", "DATETIME",
            ["'2024-01-01 00:00:00'", "'2024-04-01 00:00:00'",
             "'2024-07-01 00:00:00'", "'2024-10-01 00:00:00'", "'2025-01-01 00:00:00'"],
            "'2024-05-15 12:00:00'",
            ["'2024-02-15 00:00:00'", "'2024-05-15 12:00:00'",
             "'2024-08-15 00:00:00'", "'2024-11-15 00:00:00'"])
    runRangeCase("datev2", "DATEV2",
            ["'2024-01-01'", "'2024-04-01'", "'2024-07-01'", "'2024-10-01'", "'2025-01-01'"],
            "'2024-05-15'",
            ["'2024-02-15'", "'2024-05-15'", "'2024-08-15'", "'2024-11-15'"])
    runRangeCase("datetimev2", "DATETIMEV2(3)",
            ["'2024-01-01 00:00:00.000'", "'2024-04-01 00:00:00.000'",
             "'2024-07-01 00:00:00.000'", "'2024-10-01 00:00:00.000'", "'2025-01-01 00:00:00.000'"],
            "'2024-05-15 12:00:00.123'",
            ["'2024-02-15 00:00:00.000'", "'2024-05-15 12:00:00.123'",
             "'2024-08-15 00:00:00.000'", "'2024-11-15 00:00:00.000'"])

    runListCase("boolean", "BOOLEAN", ['"false"', '"true"'], '"true"', false)
    runListCase("tinyint", "TINYINT", ["1", "2", "3", "4"], "2")
    runListCase("smallint", "SMALLINT", ["100", "200", "300", "400"], "200")
    runListCase("int", "INT", ["10", "20", "30", "40"], "20")
    runListCase("bigint", "BIGINT", ["1000", "2000", "3000", "4000"], "2000")
    runListCase("largeint", "LARGEINT",
            ["100000000000000000001", "100000000000000000002",
             "100000000000000000003", "100000000000000000004"],
            "100000000000000000002")
    runListCase("date", "DATE",
            ["'2024-01-15'", "'2024-04-15'", "'2024-07-15'", "'2024-10-15'"],
            "'2024-04-15'")
    runListCase("datetime", "DATETIME",
            ["'2024-01-15 00:00:00'", "'2024-04-15 00:00:00'",
             "'2024-07-15 00:00:00'", "'2024-10-15 00:00:00'"],
            "'2024-04-15 00:00:00'")
    runListCase("datev2", "DATEV2",
            ["'2024-01-15'", "'2024-04-15'", "'2024-07-15'", "'2024-10-15'"],
            "'2024-04-15'")
    runListCase("datetimev2", "DATETIMEV2(3)",
            ["'2024-01-15 00:00:00.001'", "'2024-04-15 00:00:00.002'",
             "'2024-07-15 00:00:00.003'", "'2024-10-15 00:00:00.004'"],
            "'2024-04-15 00:00:00.002'")
    runListCase("char", "CHAR(8)", ["'aa'", "'bb'", "'cc'", "'dd'"], "'bb'", false)
    runListCase("varchar", "VARCHAR(16)", ["'alpha'", "'bravo'", "'charlie'", "'delta'"], "'bravo'", false)
}
