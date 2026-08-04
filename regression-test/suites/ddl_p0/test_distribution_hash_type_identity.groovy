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
    assertTrue(createStmt[0][1].toString().toLowerCase().contains("distribution_hash_type"))
    assertTrue(createStmt[0][1].toString().toLowerCase().contains("identity"))

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
    // 2. identity constraint: single integer column only
    // ---------------------------------------------------------------------
    // non-integer distribution column rejected
    sql "DROP TABLE IF EXISTS test_dist_hash_bad_type"
    test {
        sql """
            CREATE TABLE `test_dist_hash_bad_type` (
                `id` BIGINT NOT NULL,
                `name` VARCHAR(32) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 8
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "distribution_hash_type" = "identity"
            );
        """
        exception "Only supports integer distribution column"
    }

    // multiple distribution columns rejected
    sql "DROP TABLE IF EXISTS test_dist_hash_multi_col"
    test {
        sql """
            CREATE TABLE `test_dist_hash_multi_col` (
                `id1` BIGINT NOT NULL,
                `id2` BIGINT NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id1`, `id2`)
            DISTRIBUTED BY HASH(`id1`, `id2`) BUCKETS 8
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "distribution_hash_type" = "identity"
            );
        """
        exception "Only supports one distribution column"
    }

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

    assertEquals(7, sql("SELECT COUNT(*) FROM test_dist_hash_identity")[0][0] as int)

    // equality queries drive single-bucket pruning; every inserted key must be locatable.
    [0L, 1L, 7L, 8L, 513L, -1L, 1024L].each { key ->
        def rows = sql("SELECT id FROM test_dist_hash_identity WHERE id = ${key}")
        assertEquals(1, rows.size(), "equality pruning lost row id=${key}".toString())
        assertEquals(key, rows[0][0] as long)
    }

    // IN-list pruning must return all three matching keys.
    def inRows = sql("SELECT id FROM test_dist_hash_identity WHERE id IN (7, 8, 1024) ORDER BY id")
    assertEquals(3, inRows.size())
    assertEquals([7L, 8L, 1024L], inRows.collect { it[0] as long })

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
    [
        "test_dist_hash_default",
        "test_dist_hash_identity",
    ].each { tbl ->
        assertEquals(80,
                sql("SELECT COUNT(*) FROM ${tbl}")[0][0] as int, "row total mismatch for ${tbl}".toString())
        def perId = sql("SELECT id, COUNT(*) FROM ${tbl} GROUP BY id ORDER BY id")
        assertEquals(8, perId.size())
        perId.each { r ->
            assertEquals(10L, r[1] as long, "id=${r[0]} in ${tbl} must have 10 rows".toString())
        }
    }

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
    def p2Rows = sql("SELECT id FROM test_dist_hash_identity_part WHERE dt = 15 AND id = 513")
    assertEquals(1, p2Rows.size(), "ADD PARTITION did not inherit identity: row lost in p2")
    assertEquals(513L, p2Rows[0][0] as long)
    assertEquals(4, sql("SELECT COUNT(*) FROM test_dist_hash_identity_part")[0][0] as int)

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

    def coloJoin = sql("""SELECT a.id FROM test_dist_hash_colo_id1 a
                            JOIN test_dist_hash_colo_id2 b ON a.id = b.id ORDER BY a.id""")
    // intersection of the two inserted key sets: {1, 7, 8, 1024}
    assertEquals([1L, 7L, 8L, 1024L], coloJoin.collect { it[0] as long })

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
    explain {
        sql("""SELECT a.id FROM test_dist_hash_colo_id1 a
                 JOIN test_dist_hash_join_crc32 b ON a.id = b.id""")
        contains "HAS_COLO_PLAN_NODE: false"
    }

    // ---------------------------------------------------------------------
    // 8. bucket-shuffle join: an identity table joins a table with a different bucket count.
    //    The optimizer keeps the identity side on its storage layout and reshuffles the other
    //    side to that layout. The reshuffle must use the identity hash on BE (not crc32),
    //    otherwise rows land on the wrong channel and the join result is wrong.
    // ---------------------------------------------------------------------
    sql "set enable_nereids_planner=true"
    sql "set enable_bucket_shuffle_join = true"
    sql "set bucket_shuffle_downgrade_ratio = 0"

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
    // include negatives, out-of-range and boundary keys to exercise identity's negative-safe modulo
    // reshuffle across channels.
    sql """INSERT INTO test_dist_hash_bs_left VALUES
             (0, 1), (7, 2), (8, 3), (513, 4), (-1, 5), (1024, 6), (-8, 7)"""
    sql """INSERT INTO test_dist_hash_bs_right VALUES
             (7, 20), (8, 30), (513, 40), (-1, 50), (1024, 60), (-8, 70), (99, 80)"""

    explain {
        sql("""SELECT l.id, l.v, r.w FROM test_dist_hash_bs_left l
                 JOIN [shuffle] test_dist_hash_bs_right r ON l.id = r.id""")
        contains "INNER JOIN(BUCKET_SHUFFLE)"
    }

    def bsJoin = sql("""SELECT l.id, l.v, r.w FROM test_dist_hash_bs_left l
                          JOIN test_dist_hash_bs_right r ON l.id = r.id ORDER BY l.id""")
    // intersection of keys: {-8, -1, 7, 8, 513, 1024}; verify identity reshuffle keeps every match.
    assertEquals([-8L, -1L, 7L, 8L, 513L, 1024L], bsJoin.collect { it[0] as long })
    // spot-check a paired value to prove rows are joined correctly, not just counted.
    def pair513 = bsJoin.find { (it[0] as long) == 513L }
    assertEquals(4, pair513[1] as int)
    assertEquals(40, pair513[2] as int)
}
