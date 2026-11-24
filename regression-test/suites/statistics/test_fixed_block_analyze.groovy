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

suite("test_fixed_block_analyze", "nonConcurrent") {
    def variables = ["enable_fixed_block_stats_sampling", "statistics_sample_unit_rows", "enable_auto_analyze"]
    def original = variables.collectEntries { name ->
        [(name): sql("SHOW GLOBAL VARIABLES LIKE '${name}'")[0][1]]
    }
    def waitForRows = { name, reported = 8192, indexName = name ->
        def deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(10)
        while (true) {
            def data = sql_return_maparray("SHOW DATA FROM ${name}")
            if (data.any { it.IndexName == indexName && (it.RowCount as long) >= (reported as long) }) {
                break
            }
            assertTrue(System.nanoTime() < deadline,
                    "Index row count for ${name}.${indexName} was not reported: ${data}")
            sleep(1000)
        }
    }
    def checkCounts = { rows, fixedColumns ->
        assertFalse(rows.isEmpty())
        assertEquals(1, rows.collect { it[2] }.unique().size())
        rows.findAll { fixedColumns.contains(it[0]) }.each { row ->
            double n = row[2] as double
            double d = row[3] as double
            double z = row[4] as double
            assertTrue(n > 0 && z >= 0 && z <= n && d >= 0 && d <= n - z, row.toString())
        }
    }
    try {
        assertEquals("true", original["enable_fixed_block_stats_sampling"].toString())
        sql "SET GLOBAL enable_auto_analyze = false"
        sql "SET GLOBAL enable_fixed_block_stats_sampling = true"
        sql "SET GLOBAL statistics_sample_unit_rows = 8"
        sql "DROP TABLE IF EXISTS fixed_block_analyze_dup"
        sql """CREATE TABLE fixed_block_analyze_dup (k INT NOT NULL, bucket INT, v VARCHAR(32), missing INT, wide STRING)
               DUPLICATE KEY(k, bucket)
               PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (4096), PARTITION p2 VALUES LESS THAN (8192))
               DISTRIBUTED BY HASH(bucket) BUCKETS 4 PROPERTIES ("replication_num"="1")"""
        sql """INSERT INTO fixed_block_analyze_dup SELECT number, number % 17, IF(number % 4 = 0, NULL, 'a:;b'), NULL,
               REPEAT('x', 128) FROM numbers("number"="8192")"""
        waitForRows("fixed_block_analyze_dup")
        setFeConfigTemporary([statistics_max_string_column_length: 64]) {
            sql "ANALYZE TABLE fixed_block_analyze_dup WITH SAMPLE ROWS 1024 WITH SYNC"
            def rows = sql "SHOW COLUMN STATS fixed_block_analyze_dup"
            def byName = rows.collectEntries { [(it[0]): it] }
            assertEquals(["k", "bucket", "v", "missing"] as Set, byName.keySet())
            checkCounts(rows, ["k", "bucket", "v", "missing"])
            assertEquals(byName.k[2], byName.k[3])
            assertTrue((byName.bucket[3] as double) >= 10.0d && (byName.bucket[3] as double) <= 34.0d,
                    byName.bucket.toString())
            assertEquals(1.0d, byName.v[3] as double)
            double nonNullRows = (byName.v[2] as double) - (byName.v[4] as double)
            assertTrue(Math.abs((byName.v[5] as double) - 4 * nonNullRows) <= 2.0d)
            assertEquals("'a:;b':1.0", byName.v[17])
            assertEquals("''", byName.missing[17])
            assertEquals(0.0d, byName.missing[3] as double)
            assertEquals(byName.missing[2], byName.missing[4])
        }

        for (def mode : ["mow", "mor", "agg"]) {
            def name = "fixed_block_analyze_${mode}"
            def agg = mode == "agg"
            def keys = agg ? "AGGREGATE KEY(k)" : "UNIQUE KEY(k)"
            def props = agg ? "" : """, "enable_unique_key_merge_on_write"="${mode == 'mow'}" """
            sql "DROP TABLE IF EXISTS ${name}"
            sql """CREATE TABLE ${name} (k INT NOT NULL, v INT ${agg ? "SUM" : ""} NOT NULL) ${keys}
                   DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true" ${props})"""
            sql """INSERT INTO ${name} SELECT number, number % 8 FROM numbers("number"="8192")"""
            if (mode != "mow") {
                sql """INSERT INTO ${name} SELECT number, 1 FROM numbers("number"="8192")"""
            }
            waitForRows(name, mode == "mow" ? 8192 : 16384)
            if (mode == "mow") {
                sql "DELETE FROM ${name} WHERE k < 6144"
                assertEquals(2048L, sql("SELECT COUNT(1) FROM ${name}")[0][0] as long)
            }
            sql "ANALYZE TABLE ${name} WITH SAMPLE ROWS 1024 WITH SYNC"
            def rows = sql "SHOW COLUMN STATS ${name}"
            assertEquals(["k", "v"] as Set, rows.collect { it[0] } as Set)
            checkCounts(rows, ["k", "v"])
            if (mode == "mow") {
                double estRows = rows[0][2] as double
                assertTrue(estRows >= 1000.0d && estRows <= 4000.0d, "Expected post-delete MOW live rows, got ${rows}")
            } else {
                // The full-scan clause must publish the count seen after storage merge.
                sql "ANALYZE TABLE ${name} WITH SAMPLE ROWS 32768 WITH SYNC"
                def census = sql "SHOW COLUMN STATS ${name}"
                def logical = sql("SELECT COUNT(1) FROM ${name} ORDER BY 1")[0][0] as double
                census.each { stat -> assertEquals(logical, stat[2] as double) }
            }
            def key = rows.find { it[0] == "k" }
            assertNotNull(key)
            assertEquals(key[2], key[3])
            if (mode == "mow") {
                def persisted = { stats -> stats.collectEntries { [(it[0]): [it[2..5], it[7..8], it[17]]] } }
                def previous = persisted(rows)
                sql "DELETE FROM ${name} WHERE k >= 0"
                assertEquals(0L, sql("SELECT COUNT(1) FROM ${name}")[0][0] as long)
                sql "ANALYZE TABLE ${name} WITH SAMPLE ROWS 1024 WITH SYNC"
                assertEquals(previous, persisted(sql("SHOW COLUMN STATS ${name}")))
            }
        }

        // Composite key + single-column HASH bucket on AGG_KEYS and MOR
        for (def agg : [true, false]) {
            def name = agg ? "fixed_block_analyze_comp_agg" : "fixed_block_analyze_comp_mor"
            def keys = agg ? "AGGREGATE KEY(p, k)" : "UNIQUE KEY(p, k)"
            def valDef = agg ? "v BIGINT SUM NOT NULL" : "v BIGINT NOT NULL"
            def props = agg ? "" : """, "enable_unique_key_merge_on_write"="false" """
            sql "DROP TABLE IF EXISTS ${name}"
            sql """CREATE TABLE ${name} (p INT NOT NULL, k INT NOT NULL, ${valDef}) ${keys}
                   PARTITION BY RANGE(p) (PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (4))
                   DISTRIBUTED BY HASH(k) BUCKETS 8
                   PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true" ${props})"""
            sql """INSERT INTO ${name} SELECT number % 4, number, 10 FROM numbers("number"="8192")"""
            sql """INSERT INTO ${name} SELECT number % 4, number, 15 FROM numbers("number"="8192")"""
            waitForRows(name, 16384)
            sql "ANALYZE TABLE ${name} WITH SAMPLE ROWS 1024 WITH SYNC"
            def compStats = sql "SHOW COLUMN STATS ${name}"
            def compByName = compStats.collectEntries { [(it[0]): it] }
            assertEquals(["p", "k", "v"] as Set, compByName.keySet())
            checkCounts(compStats, ["p", "k", "v"])
            // Merged value must reflect post-merge 25 (AGG SUM: 10 + 15) or 15 (MOR overwrite), never stale 10
            assertEquals(agg ? "25" : "15", compByName.v[7].replace("'", ""))
            assertEquals(agg ? "25" : "15", compByName.v[8].replace("'", ""))
        }

        // RANDOM-distributed AGG_KEYS with cross-tablet partial rows
        sql "DROP TABLE IF EXISTS fixed_block_analyze_random_agg"
        sql """CREATE TABLE fixed_block_analyze_random_agg (p INT NOT NULL, k INT NOT NULL, v BIGINT SUM NOT NULL)
               AGGREGATE KEY(p, k)
               PARTITION BY RANGE(p) (PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (4))
               DISTRIBUTED BY RANDOM BUCKETS 3
               PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")"""
        for (int batch = 0; batch < 3; batch++) {
            sql """INSERT INTO fixed_block_analyze_random_agg
                   SELECT number % 4, number, 10 FROM numbers("number"="8192")"""
        }
        waitForRows("fixed_block_analyze_random_agg", 24576)
        sql "ANALYZE TABLE fixed_block_analyze_random_agg WITH SAMPLE ROWS 1024 WITH SYNC"
        def randStats = sql "SHOW COLUMN STATS fixed_block_analyze_random_agg"
        def randByName = randStats.collectEntries { [(it[0]): it] }
        assertEquals(["p", "k", "v"] as Set, randByName.keySet())
        checkCounts(randStats, ["p", "k", "v"])
        // Each (p, k) appears once per batch * 3 batches = 3 times * 10 = 30 after cross-tablet merge
        assertEquals("30", randByName.v[7].replace("'", ""))
        assertEquals("30", randByName.v[8].replace("'", ""))

        // Unpartitioned composite-key RANDOM AGG_KEYS with many overlapping rowsets (shared KEY_RANGE_BLOCK)
        sql "DROP TABLE IF EXISTS fixed_block_analyze_unpart_random_agg"
        sql """CREATE TABLE fixed_block_analyze_unpart_random_agg (tag VARCHAR(32) NOT NULL, k INT NOT NULL, v BIGINT SUM NOT NULL)
               AGGREGATE KEY(tag, k)
               DISTRIBUTED BY RANDOM BUCKETS 4
               PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")"""
        for (int batch = 0; batch < 6; batch++) {
            sql """INSERT INTO fixed_block_analyze_unpart_random_agg
                   SELECT CONCAT('t_', CAST(number % 64 AS STRING)), number, 10 FROM numbers("number"="8192")"""
        }
        waitForRows("fixed_block_analyze_unpart_random_agg", 49152)
        sql "ANALYZE TABLE fixed_block_analyze_unpart_random_agg WITH SAMPLE ROWS 1024 WITH SYNC"
        def unpartRandStats = sql "SHOW COLUMN STATS fixed_block_analyze_unpart_random_agg"
        def unpartRandByName = unpartRandStats.collectEntries { [(it[0]): it] }
        assertEquals(["tag", "k", "v"] as Set, unpartRandByName.keySet())
        checkCounts(unpartRandStats, ["tag", "k", "v"])
        assertEquals("60", unpartRandByName.v[7].replace("'", ""))
        assertEquals("60", unpartRandByName.v[8].replace("'", ""))

        // Sampled histogram collection on DUP, AGG, and RANDOM AGG
        sql """CREATE TABLE IF NOT EXISTS internal.__internal_schema.histogram_statistics (
               id VARCHAR(4096) NOT NULL,
               catalog_id VARCHAR(1024) NOT NULL,
               db_id VARCHAR(1024) NOT NULL,
               tbl_id VARCHAR(1024) NOT NULL,
               idx_id VARCHAR(1024) NOT NULL,
               col_id VARCHAR(1024) NOT NULL,
               sample_rate DOUBLE NOT NULL,
               buckets VARCHAR(65533) NOT NULL,
               update_time DATETIME NOT NULL
               ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES ("replication_num"="1")"""
        for (def histTbl : ["fixed_block_analyze_dup", "fixed_block_analyze_comp_agg",
                            "fixed_block_analyze_random_agg", "fixed_block_analyze_unpart_random_agg"]) {
            sql "TRUNCATE TABLE internal.__internal_schema.histogram_statistics"
            sql "ANALYZE TABLE ${histTbl}(k, v) WITH HISTOGRAM WITH SAMPLE ROWS 1024 WITH SYNC"
            def histRows = sql "SELECT col_id, sample_rate, buckets FROM internal.__internal_schema.histogram_statistics"
            assertEquals(["k", "v"] as Set, histRows.collect { it[0] } as Set)
            histRows.each { hr ->
                double rate = hr[1] as double
                assertTrue(rate > 0.0d && rate < 1.0d, "Expected sampled histogram rate < 1.0 for ${histTbl}, got ${hr}")
                assertTrue(hr[2].toString().contains("num_buckets"), hr.toString())
            }
        }

        // Wide-table multi-column batched sampling + per-column oversized string skip isolation
        sql "DROP TABLE IF EXISTS fixed_block_analyze_wide_batch"
        sql """CREATE TABLE fixed_block_analyze_wide_batch (
                   k BIGINT NOT NULL,
                   v1 BIGINT SUM NOT NULL,
                   v2 BIGINT MAX NOT NULL,
                   v3 BIGINT MIN NOT NULL,
                   v4 VARCHAR(64) REPLACE NOT NULL,
                   v_huge STRING REPLACE NOT NULL
               ) AGGREGATE KEY(k)
               DISTRIBUTED BY HASH(k) BUCKETS 4
               PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")"""
        sql """INSERT INTO fixed_block_analyze_wide_batch
               SELECT number, 10, number, number, CONCAT('s_', CAST(number % 100 AS STRING)), REPEAT('x', 2048)
               FROM numbers("number"="8192")"""
        sql """INSERT INTO fixed_block_analyze_wide_batch
               SELECT number, 15, number, number, CONCAT('s_', CAST(number % 100 AS STRING)), REPEAT('x', 2048)
               FROM numbers("number"="8192")"""
        waitForRows("fixed_block_analyze_wide_batch", 16384)
        sql "ANALYZE TABLE fixed_block_analyze_wide_batch WITH SAMPLE ROWS 1024 WITH SYNC"
        def wideStats = sql "SHOW COLUMN STATS fixed_block_analyze_wide_batch"
        def wideByName = wideStats.collectEntries { [(it[0]): it] }
        // v_huge (>1024 bytes) skips cleanly while k, v1, v2, v3, v4 succeed in the shared batch scan
        assertEquals(["k", "v1", "v2", "v3", "v4"] as Set, wideByName.keySet())
        assertEquals("25", wideByName.v1[7].replace("'", ""))
        assertEquals("25", wideByName.v1[8].replace("'", ""))

        // Bucket-pruned AGG_KEYS Sync MV (key repeated across 8 buckets) and MOR Sync MV
        createMV("CREATE MATERIALIZED VIEW mv_comp_agg_p AS SELECT p AS mv_p, SUM(v) AS mv_sum_v FROM fixed_block_analyze_comp_agg GROUP BY p")
        createMV("CREATE MATERIALIZED VIEW mv_comp_mor AS SELECT p AS mor_p, k AS mor_k, v + 1 AS mor_v1 FROM fixed_block_analyze_comp_mor")
        createMV("CREATE MATERIALIZED VIEW mv_rand_expr AS SELECT k % 32 AS rk, SUM(v) AS rsum FROM fixed_block_analyze_unpart_random_agg GROUP BY k % 32")
        waitForRows("fixed_block_analyze_comp_agg", 1, "mv_comp_agg_p")
        waitForRows("fixed_block_analyze_comp_mor", 1, "mv_comp_mor")
        waitForRows("fixed_block_analyze_unpart_random_agg", 1, "mv_rand_expr")
        sql "ANALYZE TABLE fixed_block_analyze_comp_agg WITH SAMPLE ROWS 1024 WITH SYNC"
        def compAggMvStats = sql "SHOW COLUMN STATS fixed_block_analyze_comp_agg(mv_p)"
        assertEquals(1, compAggMvStats.size())
        // p has only 4 distinct values across 8 buckets (32 merged MV rows total); NDV must not assume singleUniqueKey=32
        assertTrue((compAggMvStats[0][3] as double) <= 8.0d, "Bucket-pruned MV key NDV overestimated: ${compAggMvStats}")
        sql "ANALYZE TABLE fixed_block_analyze_comp_mor WITH SAMPLE ROWS 1024 WITH SYNC"
        def compMorMvStats = sql "SHOW COLUMN STATS fixed_block_analyze_comp_mor(mor_v1)"
        assertEquals(1, compMorMvStats.size())
        assertEquals("16", compMorMvStats[0][7].replace("'", ""))
        assertEquals("16", compMorMvStats[0][8].replace("'", ""))
        sql "ANALYZE TABLE fixed_block_analyze_unpart_random_agg WITH SAMPLE ROWS 1024 WITH SYNC"
        def randMvStats = sql "SHOW COLUMN STATS fixed_block_analyze_unpart_random_agg(rsum)"
        assertEquals(1, randMvStats.size())

        // Composite short key with intermediate CHAR(30) and explicit short_key = 3
        sql "DROP TABLE IF EXISTS fixed_block_analyze_char_short_key"
        sql """CREATE TABLE fixed_block_analyze_char_short_key (
                   k1 INT NOT NULL,
                   k2 CHAR(30) NOT NULL,
                   k3 INT NOT NULL,
                   v BIGINT SUM NOT NULL
               ) AGGREGATE KEY(k1, k2, k3)
               DISTRIBUTED BY HASH(k1) BUCKETS 4
               PROPERTIES ("replication_num"="1", "short_key"="3", "disable_auto_compaction"="true")"""
        sql """INSERT INTO fixed_block_analyze_char_short_key
               SELECT number % 8, CONCAT(REPEAT('a', 20), CAST(number % 9 AS STRING)), 10000 - number, 7
               FROM numbers("number"="8192")"""
        waitForRows("fixed_block_analyze_char_short_key", 8192)
        sql "ANALYZE TABLE fixed_block_analyze_char_short_key WITH SAMPLE ROWS 1024 WITH SYNC"
        assertEquals(4, (sql "SHOW COLUMN STATS fixed_block_analyze_char_short_key").size())

        // Partition-subset WITH SAMPLE is rejected; explicit PARTITION(p1) full analyze collects partition stats
        // even when enable_partition_analyze is false (default)
        test {
            sql "ANALYZE TABLE fixed_block_analyze_comp_agg PARTITION(p1) WITH SAMPLE ROWS 512 WITH SYNC"
            exception "Partition-scoped ANALYZE with sample is not supported"
        }
        sql "ANALYZE TABLE fixed_block_analyze_comp_agg PARTITION(p1) WITH SYNC"
        def p1PartStats = sql "SHOW COLUMN STATS fixed_block_analyze_comp_agg (p) PARTITION(p1)"
        assertEquals(1, p1PartStats.size())
    } finally {
        sql "DROP TABLE IF EXISTS internal.__internal_schema.histogram_statistics"
        original.each { name, value -> sql "SET GLOBAL ${name} = ${value}" }
    }
}
