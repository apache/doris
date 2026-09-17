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

suite("test_distribution_hash_type_identity") {

    // ---------------------------------------------------------------------
    // 1. DDL: create table with distribution_hash_type = identity
    // ---------------------------------------------------------------------
    sql "DROP TABLE IF EXISTS test_dist_hash_identity"
    sql """
        CREATE TABLE `test_dist_hash_identity` (
            `id` BIGINT NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """

    // SHOW CREATE TABLE round-trip: the property must be echoed back so the table can be rebuilt.
    def createStmt = sql "SHOW CREATE TABLE test_dist_hash_identity"
    assertTrue(createStmt[0][1].toString().toLowerCase()
            .contains("\"distribution_hash_type\" = \"identity\""))

    // default (property absent) is crc32: SHOW CREATE must NOT emit the property.
    sql "DROP TABLE IF EXISTS test_dist_hash_default"
    sql """
        CREATE TABLE `test_dist_hash_default` (
            `id` BIGINT NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    def defaultStmt = sql "SHOW CREATE TABLE test_dist_hash_default"
    assertFalse(defaultStmt[0][1].toString().toLowerCase().contains("distribution_hash_type"))

    // ---------------------------------------------------------------------
    // 2. identity accepts multiple distribution columns and all valid types
    // ---------------------------------------------------------------------
    sql "DROP TABLE IF EXISTS test_dist_hash_string"
    sql """
        CREATE TABLE `test_dist_hash_string` (
            `name` VARCHAR(32) NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """

    sql "DROP TABLE IF EXISTS test_dist_hash_nullable"
    sql """
        CREATE TABLE `test_dist_hash_nullable` (
            `name` VARCHAR(32) NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """

    sql "DROP TABLE IF EXISTS test_dist_hash_ipv4"
    sql """
        CREATE TABLE `test_dist_hash_ipv4` (
            `addr` IPV4 NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`addr`)
        DISTRIBUTED BY HASH(`addr`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """

    sql "DROP TABLE IF EXISTS test_dist_hash_ipv6"
    sql """
        CREATE TABLE `test_dist_hash_ipv6` (
            `addr` IPV6 NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`addr`)
        DISTRIBUTED BY HASH(`addr`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """

    sql "DROP TABLE IF EXISTS test_dist_hash_multi_col"
    sql """
        CREATE TABLE `test_dist_hash_multi_col` (
            `id` INT NOT NULL,
            `name` VARCHAR(32) NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`, `name`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """

    sql "DROP TABLE IF EXISTS test_dist_hash_typed_multi"
    sql """
        CREATE TABLE `test_dist_hash_typed_multi` (
            `d` DATE NOT NULL,
            `dt` DATETIMEV2(6) NOT NULL,
            `amount` DECIMAL(18, 2) NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`d`, `dt`)
        DISTRIBUTED BY HASH(`d`, `dt`, `amount`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """

    // invalid hash type value rejected
    sql "DROP TABLE IF EXISTS test_dist_hash_bad_value"
    test {
        sql """
            CREATE TABLE `test_dist_hash_bad_value` (
                `id` BIGINT NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 8
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "distribution_hash_type" = "murmur"
            );
        """
        exception "Invalid distribution_hash_type"
    }

    // ---------------------------------------------------------------------
    // 3. colocate: same distribution_hash_type may share a group; different hash types may not.
    //    A colocate group keeps every table on its storage layout with no reshuffle, so all
    //    members must bucket rows with the same hash function.
    // ---------------------------------------------------------------------
    // 3a. two identity tables in the same colocate group -> allowed.
    sql "DROP TABLE IF EXISTS test_dist_hash_colo_id1"
    sql "DROP TABLE IF EXISTS test_dist_hash_colo_id2"
    sql """
        CREATE TABLE `test_dist_hash_colo_id1` (
            `id` BIGINT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity",
            "colocate_with" = "test_dist_hash_cg_identity"
        );
    """
    sql """
        CREATE TABLE `test_dist_hash_colo_id2` (
            `id` BIGINT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity",
            "colocate_with" = "test_dist_hash_cg_identity"
        );
    """

    // 3b. crc32 table joining an existing identity group -> rejected on hash type mismatch.
    sql "DROP TABLE IF EXISTS test_dist_hash_colo_crc32"
    test {
        sql """
            CREATE TABLE `test_dist_hash_colo_crc32` (
                `id` BIGINT NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 8
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "colocate_with" = "test_dist_hash_cg_identity"
            );
        """
        exception "Colocate tables must have same distribution hash type"
    }

    // ---------------------------------------------------------------------
    // 4. read/write consistency: identity write then equality query must find the row.
    //    This is the core guarantee: BE buckets and FE prunes with the same hash function.
    //    Use explicit assertions (not qt_ recording) so a lost row fails loudly instead of
    //    silently recording an empty result set.
    // ---------------------------------------------------------------------
    sql """ INSERT INTO test_dist_hash_identity VALUES
                (0, 100), (1, 101), (7, 107), (8, 108), (513, 613), (-1, 200), (1024, 300) """

    qt_identity_count "SELECT COUNT(*) FROM test_dist_hash_identity"

    // Each equality query drives single-bucket pruning.
    qt_identity_eq_0 "SELECT id FROM test_dist_hash_identity WHERE id = 0"
    qt_identity_eq_1 "SELECT id FROM test_dist_hash_identity WHERE id = 1"
    qt_identity_eq_7 "SELECT id FROM test_dist_hash_identity WHERE id = 7"
    qt_identity_eq_8 "SELECT id FROM test_dist_hash_identity WHERE id = 8"
    qt_identity_eq_513 "SELECT id FROM test_dist_hash_identity WHERE id = 513"
    qt_identity_eq_negative_1 "SELECT id FROM test_dist_hash_identity WHERE id = -1"
    qt_identity_eq_1024 "SELECT id FROM test_dist_hash_identity WHERE id = 1024"

    order_qt_identity_in "SELECT id FROM test_dist_hash_identity WHERE id IN (7, 8, 1024)"

    // Non-integer and multi-column identity layouts must use the same canonical bytes in BE
    // writes, FE tablet pruning, and bucket shuffle.
    sql "INSERT INTO test_dist_hash_string VALUES ('alpha', 1), ('beta', 2)"
    qt_identity_string "SELECT name, v FROM test_dist_hash_string WHERE name = 'beta'"

    sql "INSERT INTO test_dist_hash_nullable VALUES (NULL, 9), ('x', 10)"
    qt_identity_null "SELECT v FROM test_dist_hash_nullable WHERE name <=> NULL"

    sql "INSERT INTO test_dist_hash_ipv4 VALUES (to_ipv4('1.2.3.4'), 4), (to_ipv4('10.0.0.1'), 10)"
    qt_identity_ipv4 "SELECT v FROM test_dist_hash_ipv4 WHERE addr = to_ipv4('1.2.3.4')"

    sql "INSERT INTO test_dist_hash_ipv6 VALUES (to_ipv6('::1'), 1), (to_ipv6('2001:db8::1'), 6)"
    qt_identity_ipv6 "SELECT v FROM test_dist_hash_ipv6 WHERE addr = to_ipv6('::1')"

    sql """ INSERT INTO test_dist_hash_multi_col VALUES
                (1, 'A', 10), (1, 'B', 11), (-1, 'A', 12), (2, 'BC', 13) """
    order_qt_identity_multi """
        SELECT id, name, v FROM test_dist_hash_multi_col
        WHERE id = -1 AND name = 'A'
    """

    sql """ INSERT INTO test_dist_hash_typed_multi VALUES
                ('2026-01-02', '2026-01-02 03:04:05.123456', 123.45, 7) """
    qt_identity_typed """
        SELECT v FROM test_dist_hash_typed_multi
        WHERE d = '2026-01-02'
          AND dt = '2026-01-02 03:04:05.123456'
          AND amount = 123.45
    """

    // ---------------------------------------------------------------------
    // 5. bucket data distribution: identity spreads rows evenly, crc32 does not.
    //    Insert ids 1..8 (10 rows each, 80 rows total) into a crc32 table and an identity
    //    table, both DISTRIBUTED BY HASH(id) BUCKETS 8. With this key set:
    //      - crc32(id)%8 folds ids 3 and 8 onto the same bucket and leaves one bucket empty,
    //        so the row distribution is skewed (one 20-row bucket, one 0-row bucket).
    //      - identity uses id%8 directly, mapping the 8 distinct ids onto 8 distinct buckets,
    //        so every bucket holds exactly 10 rows and none is empty.
    //    crc32(id)%8 for id=1..8 -> {1:7, 2:5, 3:3, 4:0, 5:6, 6:4, 7:2, 8:3};
    //    bucket 1 receives no id (empty) while bucket 3 gets both 3 and 8.
    //    (verify with:  select crc32(8)%8;  -> same bucket as crc32(3)%8)
    //    id%8 for id=1..8 -> {1:1, 2:2, 3:3, 4:4, 5:5, 6:6, 7:7, 8:0}: 8 buckets, 10 rows each.
        // ---------------------------------------------------------------------
    // helper: read the per-bucket RowCount via SHOW TABLETS. Each tablet maps to one bucket and
    // (single replica here) appears once, so the list of RowCounts is the per-bucket row spread.
    // RowCount is reported asynchronously, so poll until the total matches the expected row count
    // before trusting the layout.
    def bucketRowCounts = { String tbl, int expectedTotal ->
        def counts = null
        for (int attempt = 0; attempt < 60; attempt++) {
            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
            def perBucket = tablets.collect { (it["RowCount"] as String) as long }
            long total = perBucket.sum() as long
            if (total == expectedTotal) {
                counts = perBucket
                break
            }
            sleep(5000)
        }
        assertNotNull(counts, "RowCount for ${tbl} never reached ${expectedTotal}".toString())
        return counts
    }

    // truncate existing data
    // identity table: even distribution, one row per bucket per id.
    sql "TRUNCATE TABLE test_dist_hash_identity"
    // crc32 (default) table: skewed distribution with an empty bucket.
    sql "TRUNCATE TABLE test_dist_hash_default"

    // write ids 1..8, 10 rows each (v = 1..10) -> 80 rows total for both tables.
    def bucketValues = []
    (1..8).each { id ->
        (1..10).each { v -> bucketValues << "(${id}, ${v})" }
    }
    def bucketInsert = bucketValues.join(", ")
    sql "INSERT INTO test_dist_hash_default VALUES ${bucketInsert}"
    sql "INSERT INTO test_dist_hash_identity VALUES ${bucketInsert}"

    // sanity: both tables received all 80 rows with 10 rows per id (no rows dropped on write).
    qt_crc32_row_count "SELECT COUNT(*) FROM test_dist_hash_default"
    qt_identity_row_count "SELECT COUNT(*) FROM test_dist_hash_identity"
    order_qt_crc32_rows_per_id "SELECT id, COUNT(*) FROM test_dist_hash_default GROUP BY id"
    order_qt_identity_rows_per_id "SELECT id, COUNT(*) FROM test_dist_hash_identity GROUP BY id"

    // crc32: at least one bucket is empty and at least one bucket is overloaded (>10 rows),
    // because crc32(id)%8 collides ids 3 and 8 and skips one bucket for ids 1..8.
    def crc32Counts = bucketRowCounts("test_dist_hash_default", 80)
    assertTrue(crc32Counts.any { it == 0L },
            "crc32 must leave at least one empty bucket, counts=${crc32Counts}".toString())
    assertTrue(crc32Counts.any { it > 10L },
            "crc32 must overload at least one bucket (>10), counts=${crc32Counts}".toString())

    // identity: every bucket holds exactly 10 rows -> no empty bucket, perfectly even spread.
    def identityCounts = bucketRowCounts("test_dist_hash_identity", 80)
    assertEquals(8, identityCounts.size(),
            "identity should fill all 8 buckets, counts=${identityCounts}".toString())
    assertFalse(identityCounts.any { it == 0L },
            "identity must NOT leave any empty bucket, counts=${identityCounts}".toString())
    identityCounts.each { c ->
        assertEquals(10L, c as long,
                "identity bucket must hold exactly 10 rows, counts=${identityCounts}".toString())
    }

    // ---------------------------------------------------------------------
    // 6. ADD PARTITION inherits the table hash type (commit: inherit on ADD PARTITION).
    //    A partitioned identity table; manually added partitions must keep identity so
    //    writes/reads stay consistent.
    // ---------------------------------------------------------------------
    sql "DROP TABLE IF EXISTS test_dist_hash_identity_part"
    sql """
        CREATE TABLE `test_dist_hash_identity_part` (
            `id` BIGINT NOT NULL,
            `dt` INT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `dt`)
        PARTITION BY RANGE(`dt`) (
            PARTITION p1 VALUES LESS THAN ("10")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """
    // manual ADD PARTITION: DDL cannot carry distribution_hash_type, so it must be inherited.
    sql """ ALTER TABLE test_dist_hash_identity_part ADD PARTITION p2 VALUES LESS THAN ("20")
            DISTRIBUTED BY HASH(`id`) BUCKETS 8 """

    sql """ INSERT INTO test_dist_hash_identity_part VALUES (5, 5), (513, 5), (5, 15), (513, 15) """
    // rows in the newly added partition p2 (dt=15) must be found by equality pruning too;
    // if the new partition fell back to crc32, BE/FE hash mismatch would drop these rows.
    qt_identity_added_partition "SELECT id FROM test_dist_hash_identity_part WHERE dt = 15 AND id = 513"
    qt_identity_partition_count "SELECT COUNT(*) FROM test_dist_hash_identity_part"

    // Legacy planner must pass the table hash type to HashDistributionPruner as well.
    sql "set enable_nereids_planner=false"
    sql "set enable_fallback_to_original_planner=false"
    qt_legacy_identity_integer "SELECT id FROM test_dist_hash_identity WHERE id = 1"
    qt_legacy_identity_string "SELECT name, v FROM test_dist_hash_string WHERE name = 'beta'"
    qt_legacy_identity_multi """
        SELECT id, name, v FROM test_dist_hash_multi_col WHERE id = -1 AND name = 'A'
    """
    sql "set enable_nereids_planner=true"

    // ---------------------------------------------------------------------
    // 7. colocate join: two identity tables in the same colocate group join with no reshuffle.
    //    Both sides keep their storage layout (same identity hash + same bucket count), so the
    //    plan must be a COLOCATE join and the result must match the non-optimized join.
    // ---------------------------------------------------------------------
    sql "set enable_nereids_planner=true"
    sql "set disable_colocate_plan=false"

    waitForColocateGroupStable("test_dist_hash_cg_identity")

    sql "INSERT INTO test_dist_hash_colo_id1 VALUES (0), (1), (7), (8), (513), (-1), (1024)"
    sql "INSERT INTO test_dist_hash_colo_id2 VALUES (1), (7), (8), (999), (1024)"

    explain {
        sql("""SELECT a.id FROM test_dist_hash_colo_id1 a
                 JOIN test_dist_hash_colo_id2 b ON a.id = b.id""")
        contains "HAS_COLO_PLAN_NODE: true"
    }

    order_qt_identity_colocate_join """SELECT a.id FROM test_dist_hash_colo_id1 a
                                           JOIN test_dist_hash_colo_id2 b ON a.id = b.id"""

    // a crc32 table joining an identity table must NOT colocate (different hash functions).
    sql "DROP TABLE IF EXISTS test_dist_hash_join_crc32"
    sql """
        CREATE TABLE `test_dist_hash_join_crc32` (
            `id` BIGINT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql "INSERT INTO test_dist_hash_join_crc32 VALUES (1), (7), (8), (1024)"
    // Force both storage layouts through ordinary execution shuffle. Their source-table hash
    // labels must be normalized because HASH_PARTITIONED uses the execution hash algorithm.
    sql "set enable_bucket_shuffle_join = false"
    sql "set parallel_pipeline_task_num = 4"
    explain {
        sql("""SELECT a.id FROM test_dist_hash_colo_id1 a
                 JOIN [shuffle] test_dist_hash_join_crc32 b ON a.id = b.id""")
        contains "HAS_COLO_PLAN_NODE: false"
        contains "INNER JOIN(PARTITIONED)"
    }
    order_qt_mixed_hash_join """SELECT a.id FROM test_dist_hash_colo_id1 a
                                    JOIN [shuffle] test_dist_hash_join_crc32 b ON a.id = b.id"""
    // Thousands of distinct keys feed all ordinary shuffle destinations, not just channel zero.
    sql "INSERT INTO test_dist_hash_colo_id1 SELECT number + 2048 FROM numbers('number'='4096')"
    sql "INSERT INTO test_dist_hash_join_crc32 SELECT number + 2048 FROM numbers('number'='4096')"
    def mixedHashMultiInstanceSql = """
        SELECT a.id % 17 AS g, count(*), sum(a.id)
        FROM test_dist_hash_colo_id1 a
        JOIN [shuffle] test_dist_hash_join_crc32 b ON a.id = b.id
        GROUP BY g
    """
    explain {
        sql(mixedHashMultiInstanceSql)
        contains "INNER JOIN(PARTITIONED)"
        contains "HAS_COLO_PLAN_NODE: false"
    }
    order_qt_mixed_hash_partitioned_multi_instance "${mixedHashMultiInstanceSql}"
    sql "set enable_bucket_shuffle_join = true"

    // ---------------------------------------------------------------------
    // 8. bucket-shuffle join: an identity table joins a table with a different bucket count.
    //    The optimizer keeps the identity side on its storage layout and reshuffles the other
    //    side to that layout. The reshuffle must use the identity hash on BE (not crc32),
    //    otherwise rows land on the wrong channel and the join result is wrong.
    // ---------------------------------------------------------------------
    // Force multiple local destinations so CRC32 and IDENTITY cannot both degenerate to channel 0.
    sql "set parallel_pipeline_task_num = 4"
    sql "set enable_nereids_planner=true"
    sql "set enable_bucket_shuffle_join = true"
    // Keep bucket shuffle deterministic across clusters: a positive downgrade ratio may replace it
    // with a full PARTITIONED shuffle based on the bucket and parallel-instance counts.
    sql "set bucket_shuffle_downgrade_ratio = 0"

    // [shuffle] prevents these tiny test tables from choosing a broadcast join. Together with the
    // settings above, it exercises bucket shuffle without depending on table statistics.
    def bucketShuffleJoinSql = """
        SELECT l.id, l.v, r.w FROM test_dist_hash_bs_left l
        JOIN [shuffle] test_dist_hash_bs_right r ON l.id = r.id
    """

    // With this switch off, BE adds the required local exchange while building pipelines.
    sql "set enable_local_shuffle_planner = false"

    sql "DROP TABLE IF EXISTS test_dist_hash_bs_left"
    sql "DROP TABLE IF EXISTS test_dist_hash_bs_right"
    sql """
        CREATE TABLE `test_dist_hash_bs_left` (
            `id` BIGINT NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """
    sql """
        CREATE TABLE `test_dist_hash_bs_right` (
            `id` BIGINT NOT NULL,
            `w` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 5
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """
    // include negatives, out-of-range and boundary keys to exercise unsigned binary identity
    // reshuffle across channels.
    sql """INSERT INTO test_dist_hash_bs_left VALUES
             (0, 1), (7, 2), (8, 3), (513, 4), (-1, 5), (1024, 6), (-8, 7)"""
    sql """INSERT INTO test_dist_hash_bs_right VALUES
             (7, 20), (8, 30), (513, 40), (-1, 50), (1024, 60), (-8, 70), (99, 80)"""

    // Standard EXPLAIN does not expose local-exchange placement, but it must retain the same
    // bucket-shuffle join in both planning modes. Query results then validate the BE-native path.
    explain {
        sql(bucketShuffleJoinSql)
        contains "INNER JOIN(BUCKET_SHUFFLE)"
    }
    order_qt_identity_bucket_shuffle_native "${bucketShuffleJoinSql}"

    // With this switch on, FE inserts explicit local-exchange nodes into the distributed plan.
    sql "set enable_local_shuffle_planner = true"
    explain {
        sql(bucketShuffleJoinSql)
        contains "INNER JOIN(BUCKET_SHUFFLE)"
    }
    order_qt_identity_bucket_shuffle_fe "${bucketShuffleJoinSql}"
    sql "set enable_local_shuffle_planner = false"

    // Multi-column mixed-type identity bucket shuffle follows the same composition as storage.
    // A broadcast join keeps its probe-side IDENTITY bucket layout. Its CRC32 build side must not
    // erase that metadata before the result feeds another bucket-shuffle join.
    def broadcastThenBucketSql = """
        SELECT p.id, p.v, r.w
        FROM (
            SELECT a.id, a.v
            FROM test_dist_hash_bs_left a
            JOIN [broadcast] test_dist_hash_join_crc32 b ON a.id = b.id
        ) p
        JOIN [shuffle] test_dist_hash_bs_right r ON p.id = r.id
    """
    explain {
        sql(broadcastThenBucketSql)
        contains "INNER JOIN(BROADCAST)"
        contains "INNER JOIN(BUCKET_SHUFFLE)"
    }
    order_qt_identity_broadcast_then_bucket_native "${broadcastThenBucketSql}"
    sql "set enable_local_shuffle_planner = true"
    order_qt_identity_broadcast_then_bucket_fe "${broadcastThenBucketSql}"
    sql "set enable_local_shuffle_planner = false"

    sql "DROP TABLE IF EXISTS test_dist_hash_bs_multi_right"
    sql """
        CREATE TABLE `test_dist_hash_bs_multi_right` (
            `id` INT NOT NULL,
            `name` VARCHAR(32) NOT NULL,
            `w` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`, `name`) BUCKETS 7
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """
    sql """ INSERT INTO test_dist_hash_bs_multi_right VALUES
                (1, 'A', 20), (-1, 'A', 22), (2, 'BC', 23), (9, 'missing', 24) """

    explain {
        sql("""SELECT l.id, l.name FROM test_dist_hash_multi_col l
                 JOIN [shuffle] test_dist_hash_bs_multi_right r
                 ON l.id = r.id AND l.name = r.name""")
        contains "INNER JOIN(BUCKET_SHUFFLE)"
    }

    order_qt_identity_multi_bucket_shuffle """SELECT l.id, l.name, l.v, r.w
                                                   FROM test_dist_hash_multi_col l
                                                   JOIN [shuffle] test_dist_hash_bs_multi_right r
                                                   ON l.id = r.id AND l.name = r.name"""

    sql "DROP TABLE IF EXISTS test_dist_hash_nullable_right"
    sql """
        CREATE TABLE `test_dist_hash_nullable_right` (
            `name` VARCHAR(32) NULL,
            `w` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS 7
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        );
    """
    sql "INSERT INTO test_dist_hash_nullable_right VALUES (NULL, 90), ('x', 100)"
    explain {
        sql("""SELECT l.v, r.w FROM test_dist_hash_nullable l
                 JOIN [shuffle] test_dist_hash_nullable_right r ON l.name <=> r.name""")
        contains "INNER JOIN(BUCKET_SHUFFLE)"
    }
    order_qt_identity_nullable_bucket_shuffle """SELECT l.v, r.w FROM test_dist_hash_nullable l
                                                      JOIN [shuffle] test_dist_hash_nullable_right r
                                                      ON l.name <=> r.name"""

    // Set bucket shuffle requires BOTH FE local shuffle and Nereids distribute planning.
    // The window is a hash consumer of the set output; asserting only a parent join's
    // BUCKET_SHUFFLE would also pass if the whole set were re-bucketed after execution shuffle.
    sql "set enable_local_shuffle_planner = true"
    sql "set enable_nereids_distribute_planner = true"
    sql "set enable_local_shuffle = true"
    // Otherwise INTERSECT puts its smaller input first, hiding the left-basic arm.
    sql "set disable_nereids_rules = 'REORDER_INTERSECT'"
    // Keep every key reaching the exchanges; runtime-filter pruning is not the path under test.
    sql "set runtime_filter_mode = 'OFF'"
    sql "DROP TABLE IF EXISTS test_dist_hash_set_identity"
    sql "DROP TABLE IF EXISTS test_dist_hash_set_other_identity"
    sql "DROP TABLE IF EXISTS test_dist_hash_set_crc"
    sql """CREATE TABLE test_dist_hash_set_identity(id BIGINT NOT NULL)
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES('replication_num'='1', 'distribution_hash_type'='identity')"""
    sql """CREATE TABLE test_dist_hash_set_other_identity(id BIGINT NOT NULL)
        DISTRIBUTED BY HASH(id) BUCKETS 5
        PROPERTIES('replication_num'='1', 'distribution_hash_type'='identity')"""
    sql """CREATE TABLE test_dist_hash_set_crc(id BIGINT NOT NULL)
        DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO test_dist_hash_set_identity VALUES
        (-8), (-1), (0), (1), (2), (7), (8), (513), (1024), (4294967297)"""
    sql """INSERT INTO test_dist_hash_set_other_identity VALUES
        (-1), (1), (1), (7), (9), (513), (1024), (8589934593)"""
    sql """INSERT INTO test_dist_hash_set_crc SELECT * FROM test_dist_hash_set_other_identity"""

    // Inject stats only to select the intended basic child, as in bucket_shuffle_set_operation.
    // Check left and right basics, and both directions of mixed-hash target selection.
    ["identity_left", "identity_right", "mixed_identity_target", "mixed_crc_target"].each { variant ->
        def left = variant == "identity_right" ? "test_dist_hash_set_other_identity"
                : "test_dist_hash_set_identity"
        def right = variant == "identity_left" ? "test_dist_hash_set_other_identity"
                : variant == "identity_right" ? "test_dist_hash_set_identity" : "test_dist_hash_set_crc"
        def basic = variant == "mixed_crc_target" ? right : "test_dist_hash_set_identity"
        [left, right].each { table ->
            def rows = table == basic ? 10000 : 100
            // Also keep the estimated NDV large: otherwise pre-deduplication of INTERSECT /
            // EXCEPT can shrink the intended basic below its sibling and reverse the target.
            sql """ALTER TABLE ${table} MODIFY COLUMN id SET STATS
                ('row_count'='${rows}', 'ndv'='${rows}', 'min_value'='-8', 'max_value'='8589934593')"""
        }
        ["union": "UNION ALL", "intersect": "INTERSECT", "except": "EXCEPT"].each { kind, op ->
            def query = """SELECT id, row_number() OVER (PARTITION BY id ORDER BY id) rn
                FROM (SELECT id FROM ${left} ${op} SELECT id FROM ${right}) u"""
            // Golden subtree shows the set operator itself is bucketShuffle, its basic scan
            // stays direct and its other child is PhysicalDistribute. Exact hash properties
            // and remote/local thrift fields are checked by IdentitySetOperationTest in FE UT.
            explain {
                sql "shape plan " + query
                check { String plan ->
                    def setNode = "Physical${kind.capitalize()}[bucketShuffle]"
                    assertTrue(plan.contains(setNode))
                    def lines = plan.readLines()
                    def depth = { String line -> (line =~ /^-*/)[0].length() }
                    def parentOf = { int index ->
                        lines.take(index).reverse().find { depth(it) < depth(lines[index]) } ?: ""
                    }
                    int basicScan = lines.findIndexOf { it.contains("PhysicalOlapScan[${basic}]") }
                    assertTrue(basicScan >= 0)
                    assertTrue(parentOf(basicScan).contains(setNode),
                            "${basic} must stay the direct storage basic, not the exchanged side")
                    int exchange = lines.findIndexOf { it.contains("PhysicalDistribute[DistributionSpecHash]") }
                    assertTrue(exchange >= 0)
                    assertTrue(parentOf(exchange).contains(setNode),
                            "the set child, not the whole set output, must be bucket-shuffled")
                }
            }
            quickTest("set_${variant}_${kind}_shape", "explain shape plan " + query)
            quickTest("set_${variant}_${kind}_result", query, true)
        }
    }

    sql "set disable_nereids_rules = ''"
    sql "set runtime_filter_mode = 'GLOBAL'"

    // ---------------------------------------------------------------------
    // 9. Nested-loop join preserves the probe-side IDENTITY bucket layout.
    //    The CRC32 build side is broadcast and does not repartition probe rows. Feed the NLJ output
    //    into an IDENTITY bucket-shuffle join and verify both BE-native and FE-planned local exchange
    //    paths keep matching rows in the destination buckets.
    // ---------------------------------------------------------------------
    sql "DROP TABLE IF EXISTS test_identity_nlj_probe"
    sql "DROP TABLE IF EXISTS test_identity_nlj_build"
    sql "DROP TABLE IF EXISTS test_identity_nlj_bucket"

    sql """
        CREATE TABLE test_identity_nlj_probe (
            id BIGINT NOT NULL,
            v INT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        )
    """
    sql """
        CREATE TABLE test_identity_nlj_build (
            threshold INT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(threshold)
        DISTRIBUTED BY HASH(threshold) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        CREATE TABLE test_identity_nlj_bucket (
            id BIGINT NOT NULL,
            w INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 7
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "distribution_hash_type" = "identity"
        )
    """

    sql "INSERT INTO test_identity_nlj_probe VALUES (1, 1), (7, 7), (8, 8), (99, 99)"
    sql "INSERT INTO test_identity_nlj_build VALUES (10)"
    sql "INSERT INTO test_identity_nlj_bucket VALUES (1, 10), (7, 70), (8, 80), (100, 1000)"

    def nljThenBucketSql = """
        SELECT p.id, p.v, r.w
        FROM (
            SELECT a.id, a.v
            FROM test_identity_nlj_probe a
            JOIN [broadcast] test_identity_nlj_build b ON a.v < b.threshold
        ) p
        JOIN [shuffle] test_identity_nlj_bucket r ON p.id = r.id
    """

    sql "set enable_nereids_planner = true"
    sql "set enable_bucket_shuffle_join = true"
    sql "set bucket_shuffle_downgrade_ratio = 0"
    explain {
        sql(nljThenBucketSql)
        contains "NESTED LOOP JOIN"
        contains "INNER JOIN(BUCKET_SHUFFLE)"
    }

    sql "set enable_local_shuffle_planner = false"
    order_qt_identity_nlj_then_bucket_native "${nljThenBucketSql}"
    sql "set enable_local_shuffle_planner = true"
    order_qt_identity_nlj_then_bucket_fe "${nljThenBucketSql}"
}
