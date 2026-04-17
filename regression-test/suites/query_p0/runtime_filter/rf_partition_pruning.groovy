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

suite("rf_partition_pruning") {

    // ---- Profile utilities ----
    def profileAction = new ProfileAction(context)

    def getProfileByToken = { String token ->
        String profileContent = ""
        for (int attempt = 0; attempt < 15; attempt++) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileContent = profileAction.getProfile(profileItem["Profile ID"].toString())
                    break
                }
            }
            if (profileContent != "") break
            Thread.sleep(500)
        }
        return profileContent
    }

    def extractCounterSum = { String profileText, String counterName ->
        def values = (profileText =~ /-\s*${counterName}:\s*(\d+)/).collect { it[1].toLong() }
        return values.isEmpty() ? 0L : values.sum()
    }

    // Run a join query with a unique token to capture profile, then assert pruning counters.
    // rfType: runtime_filter_type hint value
    // expectedTotal: expected TotalPartitionsForRFPruning (minimum)
    // expectedPruned: expected PartitionsPrunedByRuntimeFilter (minimum, 0 means exactly 0)
    def assertPruningProfile = { String queryBody, String rfType, long expectedTotal, long expectedPruned ->
        def token = UUID.randomUUID().toString()
        sql """
            SELECT /*+ SET_VAR(runtime_filter_type='${rfType}') */ "${token}", ${queryBody}
        """
        def profile = getProfileByToken(token)
        def total = extractCounterSum(profile, "TotalPartitionsForRFPruning")
        def pruned = extractCounterSum(profile, "PartitionsPrunedByRuntimeFilter")
        logger.info("Profile [${token}]: total=${total}, pruned=${pruned}")
        assertTrue(total >= expectedTotal, "TotalPartitionsForRFPruning: expected >= ${expectedTotal}, got ${total}")
        if (expectedPruned == 0) {
            assertTrue(pruned == 0, "PartitionsPrunedByRuntimeFilter: expected 0, got ${pruned}")
        } else {
            assertTrue(pruned >= expectedPruned, "PartitionsPrunedByRuntimeFilter: expected >= ${expectedPruned}, got ${pruned}")
        }
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
    sql "set runtime_filter_type='IN_OR_BLOOM_FILTER,MIN_MAX'"
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

    // ---- Non-monotonic expression tests ----
    // Test 16: Join on abs(part_col) — target expr is NOT a SlotRef → 0 pruned
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
    sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token16}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_abs d ON abs(f.part_col) = d.dim_key
    """
    def profile16 = getProfileByToken(token16)
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
    sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token17}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_mod d ON (f.part_col % 100) = d.dim_key
    """
    def profile17 = getProfileByToken(token17)
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
    sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token18}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_nonpart d ON f.id = d.dim_id
    """
    def profile18 = getProfileByToken(token18)
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
    sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token21}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_boundary d ON f.part_col = d.dim_key
    """
    def profile21 = getProfileByToken(token21)
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
    def result22 = sql """
        SELECT /*+ SET_VAR(runtime_filter_type='IN_OR_BLOOM_FILTER') */
            "${token22}", f.id, f.part_col, f.value
        FROM rf_prune_range_int f
        JOIN rf_prune_dim_outside d ON f.part_col = d.dim_key
    """
    assertTrue(result22.isEmpty(), "Dim outside all partitions should return empty result")
    def profile22 = getProfileByToken(token22)
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
}
