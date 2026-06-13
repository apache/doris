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

// Marked nonConcurrent: this suite asserts on FE query-profile counters
// (TotalPartitionsForRFPruning / PartitionsPrunedByRuntimeFilter). Even
// though each query embeds a unique UUID token to look up its own profile,
// the FE profile list is bounded (max_query_profile_num) and shared across
// sessions, so heavy parallel traffic could evict our profile before the
// poller finds it. Running serially keeps the assertions deterministic.
suite("rf_partition_pruning", "nonConcurrent") {
    // Disable the legacy RuntimeFilterPruner: it strips RFs whose effectiveness
    // cannot be statistically verified, and the small INSERT-only tables in
    // this suite have no analyzed column stats, so the pruner would otherwise
    // drop every RF and the partition-pruning counters under test would never
    // populate.
    sql "set enable_runtime_filter_prune=false;"

    // ---- Profile utilities ----
    def profileAction = new ProfileAction(context)
    def profileCompletionStateName = "Profile Completion State"
    def profileCompletionStateComplete = "COMPLETE"

    def getProfileByToken = { String token, List requiredCounters = [] ->
        String profileContent = ""
        String profileState = ""
        for (int attempt = 0; attempt < 60; attempt++) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileState = profileItem[profileCompletionStateName]?.toString()
                    def currentProfileContent = profileAction.getProfile(profileItem["Profile ID"].toString())
                    if (currentProfileContent != "") {
                        profileContent = currentProfileContent
                    }
                    break
                }
            }
            if (profileState == profileCompletionStateComplete && profileContent != ""
                    && requiredCounters.every { profileContent.contains(it) }) {
                break
            }
            Thread.sleep(500)
        }
        return profileContent
    }

    def rfPruningCounterNames = ["TotalPartitionsForRFPruning", "PartitionsPrunedByRuntimeFilter"]

    def extractCounterSum = { String profileText, String counterName ->
        def values = (profileText =~ /-\s*${counterName}:\s*(\d+)/).collect { it[1].toLong() }
        return values.isEmpty() ? 0L : values.sum()
    }

    def rfPruningSessionVarNames = [
        "enable_runtime_filter_prune",
        "enable_runtime_filter_partition_prune",
        "runtime_filter_wait_infinitely",
        "disable_join_reorder",
        "enable_profile",
        "profile_level",
        "parallel_pipeline_task_num",
        "runtime_filter_type",
        "runtime_filter_max_in_num",
        "runtime_filter_mode"
    ]

    def rfPruningExpectedSessionVars = [
        "enable_runtime_filter_prune": "false",
        "enable_runtime_filter_partition_prune": "true",
        "runtime_filter_wait_infinitely": "true",
        "disable_join_reorder": "true",
        "enable_profile": "true",
        "profile_level": "2",
        "parallel_pipeline_task_num": "1",
        "runtime_filter_max_in_num": "1024"
    ]

    def getRfPruningSessionVars = {
        def values = [:]
        rfPruningSessionVarNames.each { String name ->
            def rows = sql "SHOW VARIABLES LIKE '${name}'"
            assertTrue(rows.size() > 0, "Session variable ${name} is not found")
            values[name] = rows[0][1].toString()
        }
        return values
    }

    def assertRfPruningSessionSettings = { String tag ->
        def values = getRfPruningSessionVars()
        logger.info("RF pruning session variables [${tag}]: ${values}")
        rfPruningExpectedSessionVars.each { String name, String expected ->
            def actual = values[name]
            if (!actual.equalsIgnoreCase(expected)) {
                logger.info("Changed session variables before RF pruning failure: "
                        + sql("SHOW VARIABLES WHERE Changed = 1"))
            }
            assertTrue(actual.equalsIgnoreCase(expected),
                    "Session variable ${name}: expected ${expected}, got ${actual}")
        }
    }

    def collectPruningDebugInfo = { String querySql, String profile ->
        def sessionVars = getRfPruningSessionVars()
        def changedVars = sql("SHOW VARIABLES WHERE Changed = 1")
        def explainRows = sql("EXPLAIN ${querySql}")
        def profileHead = profile.take(1000).replaceAll("\\s+", " ")
        return "session=${sessionVars}; changed=${changedVars}; explain=${explainRows}; "
                + "profile_length=${profile.length()}; "
                + "has_total_counter=${profile.contains('TotalPartitionsForRFPruning')}; "
                + "has_pruned_counter=${profile.contains('PartitionsPrunedByRuntimeFilter')}; "
                + "profile_head=${profileHead}"
    }

    def dumpPruningDebugInfo = { String tag, String querySql, String profile ->
        def debugInfo = collectPruningDebugInfo(querySql, profile)
        logger.info("RF pruning debug [${tag}]: ${debugInfo}")
        return debugInfo
    }

    def assertProfileHasRfPruningCounters = { String tag, String querySql, String profile ->
        def debugInfo = ""
        def missingCounters = rfPruningCounterNames.findAll { !profile.contains(it) }
        if (profile == "" || !missingCounters.isEmpty()) {
            debugInfo = dumpPruningDebugInfo(tag, querySql, profile)
        }
        assertTrue(profile != "", "Profile not found for ${tag}; ${debugInfo}")
        assertTrue(missingCounters.isEmpty(),
                "Profile missing RF pruning counters ${missingCounters} for ${tag}; ${debugInfo}")
        return debugInfo
    }

    // Run a join query with a unique token to capture profile, then assert pruning counters.
    // rfType: runtime_filter_type hint value
    // expectedTotal: expected TotalPartitionsForRFPruning (minimum)
    // expectedPruned: expected PartitionsPrunedByRuntimeFilter (minimum, 0 means exactly 0)
    def assertPruningProfile = { String queryBody, String rfType, long expectedTotal, long expectedPruned ->
        def token = UUID.randomUUID().toString()
        assertRfPruningSessionSettings("before ${token}")
        def querySql = """
            SELECT /*+ SET_VAR(runtime_filter_type='${rfType}') */ "${token}", ${queryBody}
        """
        sql querySql
        def profile = getProfileByToken(token, rfPruningCounterNames)
        def debugInfo = assertProfileHasRfPruningCounters(token, querySql, profile)
        def total = extractCounterSum(profile, "TotalPartitionsForRFPruning")
        def pruned = extractCounterSum(profile, "PartitionsPrunedByRuntimeFilter")
        logger.info("Profile [${token}]: total=${total}, pruned=${pruned}")
        if (total < expectedTotal || (expectedPruned == 0 ? pruned != 0 : pruned < expectedPruned)) {
            debugInfo = debugInfo == "" ? dumpPruningDebugInfo(token, querySql, profile) : debugInfo
        }
        assertTrue(total >= expectedTotal,
                "TotalPartitionsForRFPruning: expected >= ${expectedTotal}, got ${total}; ${debugInfo}")
        if (expectedPruned == 0) {
            assertTrue(pruned == 0, "PartitionsPrunedByRuntimeFilter: expected 0, got ${pruned}; ${debugInfo}")
        } else {
            assertTrue(pruned >= expectedPruned,
                    "PartitionsPrunedByRuntimeFilter: expected >= ${expectedPruned}, got ${pruned}; ${debugInfo}")
        }
    }

    def assertNoPartitionPruningProfile = { String queryBody, String rfType ->
        def token = UUID.randomUUID().toString()
        assertRfPruningSessionSettings("before no-prune ${token}")
        def querySql = """
            SELECT /*+ SET_VAR(runtime_filter_type='${rfType}') */ "${token}", ${queryBody}
        """
        sql querySql
        def profile = getProfileByToken(token, rfPruningCounterNames)
        def debugInfo = assertProfileHasRfPruningCounters("no-prune ${token}", querySql, profile)
        def total = extractCounterSum(profile, "TotalPartitionsForRFPruning")
        def pruned = extractCounterSum(profile, "PartitionsPrunedByRuntimeFilter")
        logger.info("No-prune profile [${token}]: total=${total}, pruned=${pruned}")
        if (total != 0 || pruned != 0) {
            debugInfo = debugInfo == "" ? dumpPruningDebugInfo("no-prune ${token}", querySql, profile) : debugInfo
        }
        assertTrue(total == 0, "TotalPartitionsForRFPruning: expected 0, got ${total}; ${debugInfo}")
        assertTrue(pruned == 0, "PartitionsPrunedByRuntimeFilter: expected 0, got ${pruned}; ${debugInfo}")
    }

    // ============================================================
    // Setup: Range-partitioned fact table (INT partition column)
    // ============================================================
    sql "drop table if exists rf_prune_range_int"
    sql """
        CREATE TABLE rf_prune_range_int (
            id INT NOT NULL,
            part_col INT NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(part_col) (
            PARTITION p1 VALUES [("0"), ("100")),
            PARTITION p2 VALUES [("100"), ("200")),
            PARTITION p3 VALUES [("200"), ("300")),
            PARTITION p4 VALUES [("300"), ("400"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """

    sql "drop table if exists rf_prune_dim_int"
    sql """
        CREATE TABLE rf_prune_dim_int (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """INSERT INTO rf_prune_range_int VALUES
        (1, 10, 'a'), (2, 20, 'b'), (3, 50, 'c'), (4, 80, 'd'), (5, 90, 'e'),
        (6, 110, 'f'), (7, 120, 'g'), (8, 150, 'h'), (9, 180, 'i'), (10, 190, 'j'),
        (11, 210, 'k'), (12, 220, 'l'), (13, 250, 'm'), (14, 280, 'n'), (15, 290, 'o'),
        (16, 310, 'p'), (17, 320, 'q'), (18, 350, 'r'), (19, 380, 's'), (20, 390, 't')
    """

    sql """INSERT INTO rf_prune_dim_int VALUES (10, 'x'), (50, 'y'), (90, 'z')"""

    // ============================================================
    // Setup: Range-partitioned fact table (DATE partition column)
    // ============================================================
    sql "drop table if exists rf_prune_range_date"
    sql """
        CREATE TABLE rf_prune_range_date (
            id INT NOT NULL,
            dt DATE NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(dt) (
            PARTITION p2024q1 VALUES [("2024-01-01"), ("2024-04-01")),
            PARTITION p2024q2 VALUES [("2024-04-01"), ("2024-07-01")),
            PARTITION p2024q3 VALUES [("2024-07-01"), ("2024-10-01")),
            PARTITION p2024q4 VALUES [("2024-10-01"), ("2025-01-01"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """

    sql "drop table if exists rf_prune_dim_date"
    sql """
        CREATE TABLE rf_prune_dim_date (
            dim_dt DATE NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_dt) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """INSERT INTO rf_prune_range_date VALUES
        (1, '2024-01-15', 'jan'), (2, '2024-02-20', 'feb'), (3, '2024-03-10', 'mar'),
        (4, '2024-04-05', 'apr'), (5, '2024-05-15', 'may'), (6, '2024-06-20', 'jun'),
        (7, '2024-07-10', 'jul'), (8, '2024-08-15', 'aug'), (9, '2024-09-20', 'sep'),
        (10, '2024-10-05', 'oct'), (11, '2024-11-15', 'nov'), (12, '2024-12-20', 'dec')
    """

    sql """INSERT INTO rf_prune_dim_date VALUES
        ('2024-01-15', 'x'), ('2024-02-20', 'y'), ('2024-03-10', 'z')
    """

    // ============================================================
    // Setup: List-partitioned fact table (VARCHAR partition column)
    // ============================================================
    sql "drop table if exists rf_prune_list_str"
    sql """
        CREATE TABLE rf_prune_list_str (
            id INT NOT NULL,
            city VARCHAR(32) NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY LIST(city) (
            PARTITION p_bj VALUES IN ("Beijing"),
            PARTITION p_sh VALUES IN ("Shanghai"),
            PARTITION p_gz VALUES IN ("Guangzhou"),
            PARTITION p_sz VALUES IN ("Shenzhen")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """

    sql "drop table if exists rf_prune_dim_city"
    sql """
        CREATE TABLE rf_prune_dim_city (
            dim_city VARCHAR(32) NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_city) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """INSERT INTO rf_prune_list_str VALUES
        (1, 'Beijing', 'a'), (2, 'Beijing', 'b'), (3, 'Beijing', 'c'),
        (4, 'Shanghai', 'd'), (5, 'Shanghai', 'e'), (6, 'Shanghai', 'f'),
        (7, 'Guangzhou', 'g'), (8, 'Guangzhou', 'h'), (9, 'Guangzhou', 'i'),
        (10, 'Shenzhen', 'j'), (11, 'Shenzhen', 'k'), (12, 'Shenzhen', 'l')
    """

    sql """INSERT INTO rf_prune_dim_city VALUES ('Beijing', 'x')"""

    // ============================================================
    // Setup: List-partitioned fact table (INT partition column)
    // ============================================================
    sql "drop table if exists rf_prune_list_int"
    sql """
        CREATE TABLE rf_prune_list_int (
            id INT NOT NULL,
            region_id INT NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY LIST(region_id) (
            PARTITION p_r1 VALUES IN ("1"),
            PARTITION p_r2 VALUES IN ("2"),
            PARTITION p_r3 VALUES IN ("3"),
            PARTITION p_r4 VALUES IN ("4"),
            PARTITION p_r5 VALUES IN ("5")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num" = "1")
    """

    sql "drop table if exists rf_prune_dim_region"
    sql """
        CREATE TABLE rf_prune_dim_region (
            dim_region INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_region) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """INSERT INTO rf_prune_list_int VALUES
        (1, 1, 'a'), (2, 1, 'b'),
        (3, 2, 'c'), (4, 2, 'd'),
        (5, 3, 'e'), (6, 3, 'f'),
        (7, 4, 'g'), (8, 4, 'h'),
        (9, 5, 'i'), (10, 5, 'j')
    """

    sql """INSERT INTO rf_prune_dim_region VALUES (1, 'x'), (3, 'y')"""

    // ============================================================
    // Session settings
    // ============================================================
    sql "set enable_runtime_filter_partition_prune=true"
    sql "set runtime_filter_type='IN_OR_BLOOM_FILTER,MIN_MAX'"
    // Some shared regression environments set runtime_filter_max_in_num=0,
    // which forces IN_OR_BLOOM runtime filters to become Bloom filters. RF
    // partition pruning needs exact IN sets for these cases.
    sql "set runtime_filter_max_in_num=1024"
    sql "set runtime_filter_wait_infinitely=true"
    sql "set disable_join_reorder=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"
    // Fix parallelism to 1 so profile counters are deterministic across environments
    sql "set parallel_pipeline_task_num=1"

    // ============================================================
    // Test 1: Range partition (INT) - IN filter prune
    // RF {10, 50, 90} → only p1 [0,100) matches → prune 3 of 4
    // ============================================================
    order_qt_range_int_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_int d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_int d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // Test 2: Range partition (INT) - MinMax filter prune
    // min=10, max=90 → only p1 matches → prune 3 of 4
    order_qt_range_int_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_int d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_int d ON f.part_col = d.dim_key",
        "MIN_MAX", 4, 3)

    // Test 3: Range partition (DATE) - IN filter prune
    // Q1 dates only → prune Q2, Q3, Q4 → prune 3 of 4
    order_qt_range_date_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.dt, f.value
        FROM rf_prune_range_date f
        JOIN rf_prune_dim_date d ON f.dt = d.dim_dt
    """
    assertPruningProfile(
        "* FROM rf_prune_range_date f JOIN rf_prune_dim_date d ON f.dt = d.dim_dt",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // Test 4: Range partition (DATE) - MinMax filter prune
    order_qt_range_date_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.dt, f.value
        FROM rf_prune_range_date f
        JOIN rf_prune_dim_date d ON f.dt = d.dim_dt
    """
    assertPruningProfile(
        "* FROM rf_prune_range_date f JOIN rf_prune_dim_date d ON f.dt = d.dim_dt",
        "MIN_MAX", 4, 3)

    // Test 5: List partition (VARCHAR) - IN filter prune
    // Only "Beijing" → prune 3 of 4
    order_qt_list_str_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.city, f.value
        FROM rf_prune_list_str f
        JOIN rf_prune_dim_city d ON f.city = d.dim_city
    """
    assertPruningProfile(
        "* FROM rf_prune_list_str f JOIN rf_prune_dim_city d ON f.city = d.dim_city",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // Test 6: List partition (INT) - IN filter prune
    // Regions {1, 3} → prune 3 of 5
    order_qt_list_int_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.region_id, f.value
        FROM rf_prune_list_int f
        JOIN rf_prune_dim_region d ON f.region_id = d.dim_region
    """
    assertPruningProfile(
        "* FROM rf_prune_list_int f JOIN rf_prune_dim_region d ON f.region_id = d.dim_region",
        "IN_OR_BLOOM_FILTER", 5, 3)

    // Test 7: No pruning - dim matches all partitions
    sql "drop table if exists rf_prune_dim_all"
    sql """
        CREATE TABLE rf_prune_dim_all (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_all VALUES (50, 'a'), (150, 'b'), (250, 'c'), (350, 'd')"""

    order_qt_no_pruning """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_all d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_all d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 4, 0)

    // Test 8: Join result correctness with aggregation
    order_qt_range_int_agg """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            count(*), min(f.part_col), max(f.part_col)
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_int d ON f.part_col = d.dim_key
    """

    // Test 9: List partition with aggregation
    order_qt_list_str_agg """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.city, count(*) as cnt
        FROM rf_prune_list_str f
        JOIN rf_prune_dim_city d ON f.city = d.dim_city
        GROUP BY f.city
    """

    // Test 10: Multiple dim rows ({2, 4}) → prune 3 of 5
    sql "drop table if exists rf_prune_dim_multi"
    sql """
        CREATE TABLE rf_prune_dim_multi (
            dim_region INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_region) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_multi VALUES (2, 'a'), (4, 'b')"""

    order_qt_list_int_multi """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.region_id, f.value
        FROM rf_prune_list_int f
        JOIN rf_prune_dim_multi d ON f.region_id = d.dim_region
    """
    assertPruningProfile(
        "* FROM rf_prune_list_int f JOIN rf_prune_dim_multi d ON f.region_id = d.dim_region",
        "IN_OR_BLOOM_FILTER", 5, 3)

    // ============================================================
    // Extended tests: more types, more partitions, non-monotonic,
    // negative ranges, boundary values
    // ============================================================

    // ---- Setup: BIGINT range, 20 partitions (0–1,000,000 step 50,000) ----
    sql "drop table if exists rf_prune_range_bigint"
    def bigintPartitions = (0..19).collect { i ->
        def lo = i * 50000L
        def hi = (i + 1) * 50000L
        "PARTITION p${i} VALUES [(\"${lo}\"), (\"${hi}\"))"
    }.join(",\n            ")
    sql """
        CREATE TABLE rf_prune_range_bigint (
            id INT NOT NULL,
            bval BIGINT NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(bval) (
            ${bigintPartitions}
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    // Insert 2 rows per partition
    def bigintInsertVals = (0..19).collect { i ->
        def v1 = i * 50000L + 100
        def v2 = i * 50000L + 25000
        "(${i * 2 + 1}, ${v1}, 'v${i}a'), (${i * 2 + 2}, ${v2}, 'v${i}b')"
    }.join(",\n        ")
    sql "INSERT INTO rf_prune_range_bigint VALUES ${bigintInsertVals}"

    // Dim: only values in p0 [0,50000) and p5 [250000,300000) → 18 pruned
    sql "drop table if exists rf_prune_dim_bigint"
    sql """
        CREATE TABLE rf_prune_dim_bigint (
            dim_key BIGINT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_bigint VALUES (100, 'x'), (25000, 'y'), (250100, 'z'), (275000, 'w')"""

    // Test 11: BIGINT range (20 partitions) + IN → 18 pruned
    order_qt_bigint_range_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.bval, f.value
        FROM rf_prune_range_bigint f
        JOIN rf_prune_dim_bigint d ON f.bval = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_bigint f JOIN rf_prune_dim_bigint d ON f.bval = d.dim_key",
        "IN_OR_BLOOM_FILTER", 20, 18)

    // Test 12: BIGINT range (20 partitions) + MinMax → 18 pruned
    // min=100, max=275000 → covers p0–p5 (6 partitions), so 14 pruned
    order_qt_bigint_range_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.bval, f.value
        FROM rf_prune_range_bigint f
        JOIN rf_prune_dim_bigint d ON f.bval = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_bigint f JOIN rf_prune_dim_bigint d ON f.bval = d.dim_key",
        "MIN_MAX", 20, 14)

    // ---- Setup: DATETIME range, 8 partitions (quarterly, 2024-2025) ----
    sql "drop table if exists rf_prune_range_datetime"
    sql """
        CREATE TABLE rf_prune_range_datetime (
            id INT NOT NULL,
            ts DATETIME NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(ts) (
            PARTITION p2024q1 VALUES [("2024-01-01 00:00:00"), ("2024-04-01 00:00:00")),
            PARTITION p2024q2 VALUES [("2024-04-01 00:00:00"), ("2024-07-01 00:00:00")),
            PARTITION p2024q3 VALUES [("2024-07-01 00:00:00"), ("2024-10-01 00:00:00")),
            PARTITION p2024q4 VALUES [("2024-10-01 00:00:00"), ("2025-01-01 00:00:00")),
            PARTITION p2025q1 VALUES [("2025-01-01 00:00:00"), ("2025-04-01 00:00:00")),
            PARTITION p2025q2 VALUES [("2025-04-01 00:00:00"), ("2025-07-01 00:00:00")),
            PARTITION p2025q3 VALUES [("2025-07-01 00:00:00"), ("2025-10-01 00:00:00")),
            PARTITION p2025q4 VALUES [("2025-10-01 00:00:00"), ("2026-01-01 00:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_range_datetime VALUES
        (1, '2024-02-15 10:00:00', 'jan'), (2, '2024-05-20 12:00:00', 'may'),
        (3, '2024-08-10 08:00:00', 'aug'), (4, '2024-11-25 14:00:00', 'nov'),
        (5, '2025-02-01 09:00:00', 'feb25'), (6, '2025-05-15 11:00:00', 'may25'),
        (7, '2025-08-20 16:00:00', 'aug25'), (8, '2025-11-10 13:00:00', 'nov25')
    """

    sql "drop table if exists rf_prune_dim_datetime"
    sql """
        CREATE TABLE rf_prune_dim_datetime (
            dim_ts DATETIME NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_ts) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // Only 2024 Q1 timestamps → 7 of 8 partitions pruned
    sql """INSERT INTO rf_prune_dim_datetime VALUES
        ('2024-02-15 10:00:00', 'x'), ('2024-03-01 00:00:00', 'y')
    """

    // Test 13: DATETIME range (8 partitions) + IN → 7 pruned
    order_qt_datetime_range_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.ts, f.value
        FROM rf_prune_range_datetime f
        JOIN rf_prune_dim_datetime d ON f.ts = d.dim_ts
    """
    assertPruningProfile(
        "* FROM rf_prune_range_datetime f JOIN rf_prune_dim_datetime d ON f.ts = d.dim_ts",
        "IN_OR_BLOOM_FILTER", 8, 7)

    // Test 14: DATETIME range (8 partitions) + MinMax → 7 pruned
    order_qt_datetime_range_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.ts, f.value
        FROM rf_prune_range_datetime f
        JOIN rf_prune_dim_datetime d ON f.ts = d.dim_ts
    """
    assertPruningProfile(
        "* FROM rf_prune_range_datetime f JOIN rf_prune_dim_datetime d ON f.ts = d.dim_ts",
        "MIN_MAX", 8, 7)

    // ---- Setup: 50 INT list partitions ----
    sql "drop table if exists rf_prune_list_50"
    def list50Partitions = (1..50).collect { i ->
        "PARTITION p_${i} VALUES IN (\"${i}\")"
    }.join(",\n            ")
    sql """
        CREATE TABLE rf_prune_list_50 (
            id INT NOT NULL,
            cat_id INT NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY LIST(cat_id) (
            ${list50Partitions}
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    def list50InsertVals = (1..50).collect { i ->
        "(${i}, ${i}, 'cat${i}')"
    }.join(", ")
    sql "INSERT INTO rf_prune_list_50 VALUES ${list50InsertVals}"

    sql "drop table if exists rf_prune_dim_50"
    sql """
        CREATE TABLE rf_prune_dim_50 (
            dim_cat INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_cat) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // Match 5 of 50 → 45 pruned
    sql """INSERT INTO rf_prune_dim_50 VALUES (3, 'a'), (10, 'b'), (25, 'c'), (40, 'd'), (50, 'e')"""

    // Test 15: 50 list partitions + IN → 45 pruned
    order_qt_list_50_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.cat_id, f.value
        FROM rf_prune_list_50 f
        JOIN rf_prune_dim_50 d ON f.cat_id = d.dim_cat
    """
    assertPruningProfile(
        "* FROM rf_prune_list_50 f JOIN rf_prune_dim_50 d ON f.cat_id = d.dim_cat",
        "IN_OR_BLOOM_FILTER", 50, 45)

    // Test 15b: LIST partition + expression target. LIST values are finite, so
    // abs(cat_id) can be projected value-by-value without monotonicity.
    assertPruningProfile(
        "* FROM rf_prune_list_50 f JOIN rf_prune_dim_50 d ON abs(f.cat_id) = d.dim_cat",
        "IN_OR_BLOOM_FILTER", 50, 45)

    // ---- Non-monotonic expression tests ----
    // Test 16: RANGE join on abs(part_col) — non-monotonic target expr → 0 pruned
    // Using existing rf_prune_range_int (4 partitions), dim has value 50
    sql "drop table if exists rf_prune_dim_abs"
    sql """
        CREATE TABLE rf_prune_dim_abs (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_abs VALUES (50, 'x')"""

    def token16 = UUID.randomUUID().toString()
    def query16 = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token16}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_abs d ON abs(f.part_col) = d.dim_key
    """
    sql query16
    def profile16 = getProfileByToken(token16, rfPruningCounterNames)
    assertProfileHasRfPruningCounters(token16, query16, profile16)
    def pruned16 = extractCounterSum(profile16, "PartitionsPrunedByRuntimeFilter")
    logger.info("non_monotonic abs: pruned=${pruned16}")
    assertTrue(pruned16 == 0, "Non-monotonic expr should not prune, got ${pruned16}")

    // Test 17: Join on f.part_col % 100 — non-monotonic → 0 pruned
    sql "drop table if exists rf_prune_dim_mod"
    sql """
        CREATE TABLE rf_prune_dim_mod (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_mod VALUES (10, 'x')"""

    def token17 = UUID.randomUUID().toString()
    def query17 = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token17}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_mod d ON (f.part_col % 100) = d.dim_key
    """
    sql query17
    def profile17 = getProfileByToken(token17, rfPruningCounterNames)
    assertProfileHasRfPruningCounters(token17, query17, profile17)
    def pruned17 = extractCounterSum(profile17, "PartitionsPrunedByRuntimeFilter")
    logger.info("non_monotonic mod: pruned=${pruned17}")
    assertTrue(pruned17 == 0, "Non-monotonic mod expr should not prune, got ${pruned17}")

    // Test 18: Join on non-partition column (f.id) → 0 partition pruning
    sql "drop table if exists rf_prune_dim_nonpart"
    sql """
        CREATE TABLE rf_prune_dim_nonpart (
            dim_id INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_nonpart VALUES (1, 'x'), (3, 'y')"""

    def token18 = UUID.randomUUID().toString()
    def query18 = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token18}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_nonpart d ON f.id = d.dim_id
    """
    sql query18
    def profile18 = getProfileByToken(token18, rfPruningCounterNames)
    assertProfileHasRfPruningCounters(token18, query18, profile18)
    def pruned18 = extractCounterSum(profile18, "PartitionsPrunedByRuntimeFilter")
    logger.info("non_partition_col: pruned=${pruned18}")
    assertTrue(pruned18 == 0, "Join on non-partition col should not prune, got ${pruned18}")

    // ---- Negative value range partitions ----
    sql "drop table if exists rf_prune_range_neg"
    sql """
        CREATE TABLE rf_prune_range_neg (
            id INT NOT NULL,
            neg_col INT NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(neg_col) (
            PARTITION pn4 VALUES [("-400"), ("-300")),
            PARTITION pn3 VALUES [("-300"), ("-200")),
            PARTITION pn2 VALUES [("-200"), ("-100")),
            PARTITION pn1 VALUES [("-100"), ("0"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_range_neg VALUES
        (1, -350, 'a'), (2, -310, 'b'),
        (3, -250, 'c'), (4, -210, 'd'),
        (5, -150, 'e'), (6, -110, 'f'),
        (7, -50, 'g'), (8, -10, 'h')
    """

    sql "drop table if exists rf_prune_dim_neg"
    sql """
        CREATE TABLE rf_prune_dim_neg (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // Only values in pn1 [-100, 0) → prune 3 of 4
    sql """INSERT INTO rf_prune_dim_neg VALUES (-50, 'x'), (-10, 'y')"""

    // Test 19: Negative range + IN → 3 pruned
    order_qt_neg_range_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.neg_col, f.value
        FROM rf_prune_range_neg f
        JOIN rf_prune_dim_neg d ON f.neg_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_neg f JOIN rf_prune_dim_neg d ON f.neg_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // Test 20: Negative range + MinMax → 3 pruned
    order_qt_neg_range_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.neg_col, f.value
        FROM rf_prune_range_neg f
        JOIN rf_prune_dim_neg d ON f.neg_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_neg f JOIN rf_prune_dim_neg d ON f.neg_col = d.dim_key",
        "MIN_MAX", 4, 3)

    // ---- Boundary value tests ----
    // Dim has exact partition boundary values: 100, 200, 300
    // 100 ∈ p2 [100,200), 200 ∈ p3 [200,300), 300 ∈ p4 [300,400)
    // Only p1 [0,100) is pruned → 1 pruned
    sql "drop table if exists rf_prune_dim_boundary"
    sql """
        CREATE TABLE rf_prune_dim_boundary (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_boundary VALUES (100, 'a'), (200, 'b'), (300, 'c')"""

    def token21 = UUID.randomUUID().toString()
    def query21 = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token21}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_boundary d ON f.part_col = d.dim_key
    """
    sql query21
    def profile21 = getProfileByToken(token21, rfPruningCounterNames)
    assertProfileHasRfPruningCounters(token21, query21, profile21)
    def pruned21 = extractCounterSum(profile21, "PartitionsPrunedByRuntimeFilter")
    def total21 = extractCounterSum(profile21, "TotalPartitionsForRFPruning")
    logger.info("boundary_exact: total=${total21}, pruned=${pruned21}")
    // p1 is pruned (no value 0-99 in dim), p2/p3/p4 match boundary values
    assertTrue(pruned21 >= 1, "Boundary test: expected >= 1 pruned, got ${pruned21}")

    // Test 22: Dim values outside all partitions → all pruned, empty result
    sql "drop table if exists rf_prune_dim_outside"
    sql """
        CREATE TABLE rf_prune_dim_outside (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_outside VALUES (500, 'x'), (600, 'y'), (700, 'z')"""

    def token22 = UUID.randomUUID().toString()
    def query22 = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token22}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_outside d ON f.part_col = d.dim_key
    """
    def result22 = sql query22
    assertTrue(result22.isEmpty(), "Dim outside all partitions should return empty result")
    def profile22 = getProfileByToken(token22, rfPruningCounterNames)
    assertProfileHasRfPruningCounters(token22, query22, profile22)
    def pruned22 = extractCounterSum(profile22, "PartitionsPrunedByRuntimeFilter")
    logger.info("outside_all: pruned=${pruned22}")
    // All 4 partitions should be pruned since RF IN {500,600,700} intersects none
    assertTrue(pruned22 >= 4, "All partitions should be pruned, got ${pruned22}")

    // ---- Descending dim value order test ----
    // Verify that dim insertion order (desc vs asc) doesn't affect pruning
    sql "drop table if exists rf_prune_dim_desc"
    sql """
        CREATE TABLE rf_prune_dim_desc (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // Same values as rf_prune_dim_int {10, 50, 90} but inserted in descending order
    sql """INSERT INTO rf_prune_dim_desc VALUES (90, 'z'), (50, 'y'), (10, 'x')"""

    // Test 23: Descending dim order + IN → should still prune 3 of 4
    order_qt_desc_dim_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_desc d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_desc d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // Test 24: Descending dim order + MinMax → should still prune 3 of 4
    order_qt_desc_dim_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_desc d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_desc d ON f.part_col = d.dim_key",
        "MIN_MAX", 4, 3)

    // ---- Combined RF types test ----
    // Test 25: Both IN and MinMax filters simultaneously
    order_qt_combined_rf """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER,MIN_MAX') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_int d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_int d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER,MIN_MAX", 4, 3)

    // ---- Multi-partition match with MinMax spanning multiple partitions ----
    // Dim values span p1 and p3: {50, 250} → MinMax [50, 250] covers p1, p2, p3
    // IN filter prunes p2, p4 (2 pruned). MinMax prunes only p4 (1 pruned).
    sql "drop table if exists rf_prune_dim_span"
    sql """
        CREATE TABLE rf_prune_dim_span (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_span VALUES (50, 'x'), (250, 'y')"""

    // Test 26: Spanning dim + IN → 2 pruned (p2, p4 don't contain 50 or 250)
    order_qt_span_in """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_span d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_span d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 4, 2)

    // Test 27: Spanning dim + MinMax → [50,250] covers p1,p2,p3; only p4 pruned → 1 pruned
    order_qt_span_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_span d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_int f JOIN rf_prune_dim_span d ON f.part_col = d.dim_key",
        "MIN_MAX", 4, 1)

    // ============================================================
    // Tests 28-32: LIST partition with NULL key (only_null & mixed)
    // ============================================================
    sql "drop table if exists rf_prune_list_null"
    sql """
        CREATE TABLE rf_prune_list_null (
            id INT NOT NULL,
            part_col INT NULL,
            value VARCHAR(64)
        )
        PARTITION BY LIST(part_col) (
            PARTITION p_null VALUES IN ((NULL)),
            PARTITION p_one VALUES IN ("1"),
            PARTITION p_two VALUES IN ("2"),
            PARTITION p_three VALUES IN ("3")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_list_null VALUES
        (1, NULL, 'a'), (2, NULL, 'b'),
        (3, 1, 'c'), (4, 1, 'd'),
        (5, 2, 'e'),
        (6, 3, 'f')"""

    sql "drop table if exists rf_prune_dim_one"
    sql """
        CREATE TABLE rf_prune_dim_one (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_one VALUES (1, 'x')"""

    // Test 28: Non-null-aware RF {1} on LIST{p_null,p_one,p_two,p_three}
    //   p_null is only_null → pruned (RF !contain NULL)
    //   p_two, p_three → pruned (no value match)
    //   p_one → kept
    order_qt_list_only_null_pruned """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col, f.value
        FROM rf_prune_list_null f
        JOIN rf_prune_dim_one d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_list_null f JOIN rf_prune_dim_one d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // Test 29: Same with MIN_MAX → MinMax [1,1] excludes p_null/p_two/p_three → 3 pruned
    order_qt_list_only_null_minmax """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */
            f.id, f.part_col, f.value
        FROM rf_prune_list_null f
        JOIN rf_prune_dim_one d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_list_null f JOIN rf_prune_dim_one d ON f.part_col = d.dim_key",
        "MIN_MAX", 4, 3)

    // Test 30: Mixed NULL+value partition. Build a list where one partition holds {NULL, 5}.
    sql "drop table if exists rf_prune_list_mixed"
    sql """
        CREATE TABLE rf_prune_list_mixed (
            id INT NOT NULL,
            part_col INT NULL,
            value VARCHAR(64)
        )
        PARTITION BY LIST(part_col) (
            PARTITION p_a VALUES IN ((NULL), ("5")),
            PARTITION p_b VALUES IN (("10")),
            PARTITION p_c VALUES IN (("20"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_list_mixed VALUES
        (1, NULL, 'a'), (2, 5, 'b'),
        (3, 10, 'c'),
        (4, 20, 'd')"""

    sql "drop table if exists rf_prune_dim_five"
    sql """
        CREATE TABLE rf_prune_dim_five (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_five VALUES (5, 'x')"""

    // Test 30: Mixed partition {NULL,5} + RF {5} (non-null-aware) → p_a kept, p_b/p_c pruned
    // Crucial regression: validates that concrete value 5 survives the NULL marker.
    order_qt_list_mixed_value_kept """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col, f.value
        FROM rf_prune_list_mixed f
        JOIN rf_prune_dim_five d ON f.part_col = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_list_mixed f JOIN rf_prune_dim_five d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 3, 2)

    // Test 31: Mixed partition {NULL,5} + RF {7} (no value match, RF non-null-aware)
    //   p_a still pruned (NULL row can't match non-null RF; concrete 5 != 7)
    //   p_b, p_c pruned
    sql "drop table if exists rf_prune_dim_seven"
    sql """
        CREATE TABLE rf_prune_dim_seven (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_seven VALUES (7, 'x')"""

    def token_mixed_all = UUID.randomUUID().toString()
    def res_mixed_all = sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token_mixed_all}", f.id
        FROM rf_prune_list_mixed f
        JOIN rf_prune_dim_seven d ON f.part_col = d.dim_key
    """
    assertTrue(res_mixed_all.isEmpty(), "RF {7} matches no row, expect empty")
    assertPruningProfile(
        "* FROM rf_prune_list_mixed f JOIN rf_prune_dim_seven d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 3, 3)

    // Test 32: Null-safe equal join (<=>) on mixed partition. RF is null_aware AND
    //   contains NULL (build side has NULL key), so p_a (which contains NULL) MUST
    //   be kept even if RF concrete values miss 5.
    sql "drop table if exists rf_prune_dim_null"
    sql """
        CREATE TABLE rf_prune_dim_null (
            dim_key INT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_val) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_null VALUES (NULL, 'n')"""

    order_qt_list_mixed_nullsafe """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col
        FROM rf_prune_list_mixed f
        JOIN rf_prune_dim_null d ON f.part_col <=> d.dim_key
    """

    // Test 32b: Nullable RANGE partition columns can store NULL rows in the
    // MINVALUE-side first partition. A null-aware RF containing only NULL must
    // keep that partition even though its non-NULL value set is empty.
    sql "set allow_partition_column_nullable = true;"
    sql "drop table if exists rf_prune_range_nullable"
    sql """
        CREATE TABLE rf_prune_range_nullable (
            id INT NOT NULL,
            part_col INT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(part_col) (
            PARTITION p_low VALUES LESS THAN ("10"),
            PARTITION p_mid VALUES LESS THAN ("20"),
            PARTITION p_high VALUES LESS THAN ("30")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_range_nullable VALUES
        (1, NULL, 'n'), (2, 5, 'a'), (3, 15, 'b'), (4, 25, 'c')"""
    def rangeNullableNullsafeRows = sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.part_col, f.value
        FROM rf_prune_range_nullable f
        JOIN rf_prune_dim_null d ON f.part_col <=> d.dim_key
        ORDER BY f.id
    """
    assertEquals([[1, null, "n"]], rangeNullableNullsafeRows)
    assertPruningProfile(
        "* FROM rf_prune_range_nullable f JOIN rf_prune_dim_null d ON f.part_col <=> d.dim_key",
        "IN_OR_BLOOM_FILTER", 3, 2)

    // ============================================================
    // Tests 33-34: Multi-column RANGE projection (closed [L1, U1])
    // ============================================================
    // Critical: partition [(1,1),(1,5)) only contains rows whose first key == 1.
    // Naively projecting to [1,1) (half-open) would mark it as empty and wrongly
    // prune it when RF {1} probes. The fix uses closed [1,1].
    sql "drop table if exists rf_prune_range_multi"
    sql """
        CREATE TABLE rf_prune_range_multi (
            id INT NOT NULL,
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(k1, k2) (
            PARTITION p_a VALUES [("1", "1"), ("1", "5")),
            PARTITION p_b VALUES [("1", "5"), ("2", "0")),
            PARTITION p_c VALUES [("2", "0"), ("3", "0"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_range_multi VALUES
        (1, 1, 1, 'a'), (2, 1, 4, 'b'),
        (3, 1, 5, 'c'), (4, 1, 9, 'd'),
        (5, 2, 0, 'e'), (6, 2, 9, 'f')"""

    // Test 33: RF {1} probing k1 → must keep p_a AND p_b (both span first-col=1),
    //   prune p_c. Validates closed-upper-bound projection.
    order_qt_range_multi_first_col """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.k1, f.k2, f.value
        FROM rf_prune_range_multi f
        JOIN rf_prune_dim_one d ON f.k1 = d.dim_key
    """
    assertPruningProfile(
        "* FROM rf_prune_range_multi f JOIN rf_prune_dim_one d ON f.k1 = d.dim_key",
        "IN_OR_BLOOM_FILTER", 3, 1)

    // Test 34: MIN_MAX RF {1} → MinMax [1,1] still hits both p_a and p_b, prune p_c
    assertPruningProfile(
        "* FROM rf_prune_range_multi f JOIN rf_prune_dim_one d ON f.k1 = d.dim_key",
        "MIN_MAX", 3, 1)

    // Test 34b: RF target on a non-first RANGE partition column must not drive
    // partition pruning, because RANGE boundaries are only serialized for the
    // first partition column.
    def rangeMultiSecondColRows = sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            f.id, f.k1, f.k2, f.value
        FROM rf_prune_range_multi f
        JOIN rf_prune_dim_one d ON f.k2 = d.dim_key
        ORDER BY f.id
    """
    assertEquals([[1, 1, 1, "a"]], rangeMultiSecondColRows)
    assertNoPartitionPruningProfile(
        "f.id FROM rf_prune_range_multi f JOIN rf_prune_dim_one d ON f.k2 = d.dim_key",
        "IN_OR_BLOOM_FILTER")

    // ============================================================
    // Tests 35-41: Type coverage
    // ============================================================
    // Each test creates a 4-partition RANGE table over a different type and
    // joins with a 1-row dim that should match exactly one partition.
    def runTypeCoverage = { String suffix, String partType, String dimType,
                            String partLits, String dimVal, List<String> rowVals ->
        sql "drop table if exists rf_prune_type_${suffix}"
        sql """
            CREATE TABLE rf_prune_type_${suffix} (
                id INT NOT NULL,
                part_col ${partType} NOT NULL,
                value VARCHAR(64)
            )
            PARTITION BY RANGE(part_col) (
                PARTITION p1 VALUES [(${partLits.split('\\|')[0]}), (${partLits.split('\\|')[1]})),
                PARTITION p2 VALUES [(${partLits.split('\\|')[1]}), (${partLits.split('\\|')[2]})),
                PARTITION p3 VALUES [(${partLits.split('\\|')[2]}), (${partLits.split('\\|')[3]})),
                PARTITION p4 VALUES [(${partLits.split('\\|')[3]}), (${partLits.split('\\|')[4]}))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES("replication_num" = "1")
        """
        def insertRows = rowVals.withIndex().collect { v, i -> "(${i + 1}, ${v}, 'r${i}')" }.join(", ")
        sql "INSERT INTO rf_prune_type_${suffix} VALUES ${insertRows}"
        sql "drop table if exists rf_prune_dim_type_${suffix}"
        sql """
            CREATE TABLE rf_prune_dim_type_${suffix} (
                dim_key ${dimType} NOT NULL,
                dim_val VARCHAR(32)
            )
            DISTRIBUTED BY HASH(dim_val) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql "INSERT INTO rf_prune_dim_type_${suffix} VALUES (${dimVal}, 'x')"
        assertPruningProfile(
            "* FROM rf_prune_type_${suffix} f JOIN rf_prune_dim_type_${suffix} d ON f.part_col = d.dim_key",
            "IN_OR_BLOOM_FILTER", 4, 3)
    }

    // Test 35: TINYINT
    runTypeCoverage("tinyint", "TINYINT", "TINYINT",
        '"-100"|"-50"|"0"|"50"|"100"', "-75",
        ["-90", "-60", "-25", "10", "25", "75"])

    // Test 36: SMALLINT
    runTypeCoverage("smallint", "SMALLINT", "SMALLINT",
        '"0"|"1000"|"2000"|"3000"|"4000"', "1500",
        ["100", "1500", "2500", "3500"])

    // Test 37: LARGEINT
    runTypeCoverage("largeint", "LARGEINT", "LARGEINT",
        '"0"|"100000000000"|"200000000000"|"300000000000"|"400000000000"',
        "150000000000",
        ["50000000000", "150000000000", "250000000000", "350000000000"])

    // Test 38: DECIMAL is intentionally skipped — Doris does not allow
    // DECIMAL columns as RANGE partition keys, and the rest of this suite
    // already exercises decimal-typed RF targets via casts.

    // Test 39: DATE
    runTypeCoverage("date", "DATE", "DATE",
        '"2023-01-01"|"2023-04-01"|"2023-07-01"|"2023-10-01"|"2024-01-01"',
        '"2023-05-15"',
        ['"2023-02-15"', '"2023-05-15"', '"2023-08-15"', '"2023-11-15"'])

    // Test 40: DATETIME
    runTypeCoverage("datetime", "DATETIME", "DATETIME",
        '"2023-01-01 00:00:00"|"2023-04-01 00:00:00"|"2023-07-01 00:00:00"|"2023-10-01 00:00:00"|"2024-01-01 00:00:00"',
        '"2023-05-15 12:00:00"',
        ['"2023-02-15 00:00:00"', '"2023-05-15 12:00:00"', '"2023-08-15 00:00:00"', '"2023-11-15 00:00:00"'])

    // Test 41: VARCHAR is intentionally skipped — Doris does not allow
    // VARCHAR columns as RANGE partition keys.

    // ============================================================
    // Tests 42-50: Expression target pruning
    //
    // Identity partition slots still drive pruning without monotonicity metadata.
    // Non-identity target expressions must be rooted on a partition column and be
    // locally monotonic on each selected RANGE partition. Unsupported expressions
    // must not serialize partition boundaries for that RF target.
    //
    // NOTE: Nereids' RuntimeFilterPushDownVisitor itself only allows
    // non-trivial target expressions when the input is either (a) a numeric
    // single-slot expression, or (b) a chain of Cast wrapping a single slot.
    // Date functions like year(dt)/date_trunc(dt,...) on a DATE partition
    // column do not currently produce a runtime filter at all, so they cannot
    // be exercised here regardless of the classifier choice.
    // ============================================================

    sql "drop table if exists rf_prune_dim_bigint"
    sql """
        CREATE TABLE rf_prune_dim_bigint (
            dim_key BIGINT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_bigint VALUES (50, 'x')"""

    // Test 42: numeric Cast is not locally monotonic according to the current
    // Nereids Cast implementation, so it must not drive partition pruning.
    assertNoPartitionPruningProfile(
        "f.id FROM rf_prune_range_int f JOIN rf_prune_dim_bigint d "
                + "ON cast(f.part_col as bigint) = d.dim_key",
        "IN_OR_BLOOM_FILTER")

    // Test 43: chained Cast is not handled by the local-monotonic classifier yet.
    assertNoPartitionPruningProfile(
        "f.id FROM rf_prune_range_int f JOIN rf_prune_dim_int d "
                + "ON cast(cast(f.part_col as bigint) as int) = d.dim_key",
        "IN_OR_BLOOM_FILTER")

    // Test 44: target_expr = cast(dt as datetime) on a DATE partition column.
    // Cast on DATE->DATETIME is locally monotonic on every selected RANGE
    // partition, so RF {'2024-02-20'} should keep Q1 and prune Q2-Q4.
    sql "drop table if exists rf_prune_dim_dt"
    sql """
        CREATE TABLE rf_prune_dim_dt (
            dim_dt DATETIME NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_dt) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_dt VALUES ('2024-02-20 00:00:00', 'x')"""
    assertPruningProfile(
        "f.id FROM rf_prune_range_date f JOIN rf_prune_dim_dt d "
                + "ON cast(f.dt as datetime) = d.dim_dt",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // Test 45: target_expr = -part_col (Negative). Numeric input slot lets
    // the RF generator emit a filter, but Negative is not declared Monotonic
    // in Nereids → classifier returns NON_MONOTONIC → no partition pruning.
    sql "drop table if exists rf_prune_dim_neg2"
    sql """
        CREATE TABLE rf_prune_dim_neg2 (
            dim_key INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_neg2 VALUES (-50, 'x')"""
    assertNoPartitionPruningProfile(
        "f.id FROM rf_prune_range_int f JOIN rf_prune_dim_neg2 d ON (-f.part_col) = d.dim_key",
        "IN_OR_BLOOM_FILTER")

    // Test 46: Test for non-monotonic Add was removed because Nereids
    // constant-folds `part_col + 10 = 60` into `part_col = 50`, so the RF
    // ends up identity and pruning happens. The Negative case in Test 45
    // already covers the non-monotonic single-slot probe path.

    // Test 47: Cast wrapping a non-partition column - cast(f.id as bigint).
    // Cast is monotonic, but f.id is not a partition column of the table
    // (rf_prune_range_int is partitioned on part_col). Classifier walks the
    // Cast, lands on a non-partition slot, returns NON_MONOTONIC → no
    // partition pruning even though the chain is monotonic.
    sql "drop table if exists rf_prune_dim_id_bigint"
    sql """
        CREATE TABLE rf_prune_dim_id_bigint (
            dim_key BIGINT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_id_bigint VALUES (5, 'x')"""
    assertNoPartitionPruningProfile(
        "f.id FROM rf_prune_range_int f JOIN rf_prune_dim_id_bigint d "
                + "ON cast(f.id as bigint) = d.dim_key",
        "IN_OR_BLOOM_FILTER")

    // Test 48: implicit target-side widening cast. The final legacy target
    // expression contains a Cast, but the original Nereids scan target is a bare
    // Slot, so the classifier must not treat the late-added Cast as proven safe.
    assertNoPartitionPruningProfile(
        "f.id FROM rf_prune_range_int f JOIN rf_prune_dim_bigint d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER")

    // Test 49: automatic partition expression boundaries are in expression
    // domain. Until FE/BE model that domain explicitly, RF partition pruning must
    // not use them as base-column boundaries.
    sql "drop table if exists rf_prune_auto_expr_date"
    sql """
        CREATE TABLE rf_prune_auto_expr_date (
            id INT NOT NULL,
            dt DATEV2 NOT NULL,
            value VARCHAR(64)
        )
        DUPLICATE KEY(id, dt)
        AUTO PARTITION BY RANGE (date_trunc(dt, 'month')) ()
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_auto_expr_date VALUES
        (1, '2024-01-15', 'jan'), (2, '2024-02-20', 'feb'), (3, '2024-03-10', 'mar')"""
    sql "drop table if exists rf_prune_dim_datev2"
    sql """
        CREATE TABLE rf_prune_dim_datev2 (
            dim_dt DATEV2 NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_dt) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_datev2 VALUES ('2024-02-20', 'x')"""
    assertNoPartitionPruningProfile(
        "f.id FROM rf_prune_auto_expr_date f JOIN rf_prune_dim_datev2 d ON f.dt = d.dim_dt",
        "IN_OR_BLOOM_FILTER")

    // Test 50: LIST pruning accepts target expressions that reference the same
    // partition slot more than once. FE classifies this as a unique partition
    // slot target; BE must not reject the repeated VSlotRef while projecting
    // LIST partition values through the target expression.
    sql "drop table if exists rf_prune_dim_region_twice"
    sql """
        CREATE TABLE rf_prune_dim_region_twice (
            dim_region INT NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_region) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_region_twice VALUES (2, 'a'), (6, 'b')"""
    assertPruningProfile(
        "count(*) FROM rf_prune_list_int f JOIN rf_prune_dim_region_twice d "
                + "ON f.region_id + f.region_id = d.dim_region",
        "IN_OR_BLOOM_FILTER", 5, 3)

    // ============================================================
    // Test 51: String partition column (LIST partition on VARCHAR).
    //
    // Regression coverage for the BE pruner string path. ColumnValueRange
    // for string types uses CppType=std::string while RF literals/HybridSet
    // expose StringRef bytes; the pruner must construct std::string from
    // those bytes (not reinterpret_cast). Without that fix this case would
    // read garbage / crash in BE under ASAN. We exercise both IN_FILTER (via
    // small build side) and BLOOM/MIN_MAX paths by forcing IN_OR_BLOOM_FILTER.
    // ============================================================
    sql "drop table if exists rf_prune_list_str"
    sql """
        CREATE TABLE rf_prune_list_str (
            id INT,
            part_col VARCHAR(16) NOT NULL
        )
        PARTITION BY LIST(part_col) (
            PARTITION pa VALUES IN ('a','b'),
            PARTITION pc VALUES IN ('c','d'),
            PARTITION pe VALUES IN ('e','f'),
            PARTITION pg VALUES IN ('g','h')
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_list_str VALUES
        (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h')"""

    sql "drop table if exists rf_prune_dim_str"
    sql """
        CREATE TABLE rf_prune_dim_str (
            dim_key VARCHAR(16) NOT NULL,
            dim_val VARCHAR(32)
        )
        DISTRIBUTED BY HASH(dim_key) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_dim_str VALUES ('c', 'x')"""

    // RF {'c'} should keep only partition pc and prune the other three.
    assertPruningProfile(
        "* FROM rf_prune_list_str f JOIN rf_prune_dim_str d ON f.part_col = d.dim_key",
        "IN_OR_BLOOM_FILTER", 4, 3)

    // ============================================================
    // Test 52: Grouped RF with multiple targets.
    //
    // The RF generated from d.dim_key -> f1.part_col is expanded through the
    // inner join f1.part_col = f2.part_col, so Nereids creates two RF objects
    // with the same source/type/builder and different scan targets. The legacy
    // translator groups them into one RuntimeFilter with two targets. Both
    // targets must retain partition-pruning monotonicity.
    // ============================================================
    sql "drop table if exists rf_prune_range_int_copy"
    sql """
        CREATE TABLE rf_prune_range_int_copy (
            id INT NOT NULL,
            part_col INT NOT NULL,
            value VARCHAR(64)
        )
        PARTITION BY RANGE(part_col) (
            PARTITION p1 VALUES [("0"), ("100")),
            PARTITION p2 VALUES [("100"), ("200")),
            PARTITION p3 VALUES [("200"), ("300")),
            PARTITION p4 VALUES [("300"), ("400"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO rf_prune_range_int_copy VALUES
        (1, 10, 'a'), (2, 20, 'b'), (3, 50, 'c'), (4, 80, 'd'), (5, 90, 'e'),
        (6, 110, 'f'), (7, 120, 'g'), (8, 150, 'h'), (9, 180, 'i'), (10, 190, 'j'),
        (11, 210, 'k'), (12, 220, 'l'), (13, 250, 'm'), (14, 280, 'n'), (15, 290, 'o'),
        (16, 310, 'p'), (17, 320, 'q'), (18, 350, 'r'), (19, 380, 's'), (20, 390, 't')
    """
    assertPruningProfile(
        "count(*) FROM rf_prune_range_int f1 "
                + "JOIN rf_prune_range_int_copy f2 ON f1.part_col = f2.part_col "
                + "JOIN rf_prune_dim_int d ON d.dim_key = f1.part_col",
        "IN_OR_BLOOM_FILTER", 8, 6)

    // ============================================================
    // Test 53: Switch off enable_runtime_filter_partition_prune -> no pruning
    // ============================================================
    sql "set enable_runtime_filter_partition_prune=false"
    def token_off = UUID.randomUUID().toString()
    def query_off = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token_off}", f.id
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_int d ON f.part_col = d.dim_key
    """
    sql query_off
    def profile_off = getProfileByToken(token_off, rfPruningCounterNames)
    assertProfileHasRfPruningCounters(token_off, query_off, profile_off)
    def total_off = extractCounterSum(profile_off, "TotalPartitionsForRFPruning")
    def pruned_off = extractCounterSum(profile_off, "PartitionsPrunedByRuntimeFilter")
    logger.info("switch_off: total=${total_off}, pruned=${pruned_off}")
    assertTrue(total_off == 0,
        "When switched off, no partitions should be considered (got total=${total_off})")
    assertTrue(pruned_off == 0,
        "When switched off, no partitions should be pruned (got pruned=${pruned_off})")
    sql "set enable_runtime_filter_partition_prune=true"
}
