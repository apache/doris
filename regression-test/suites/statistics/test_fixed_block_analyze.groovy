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
    def waitForRows = { name, reported = 8192 ->
        def deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(10)
        while (true) {
            def data = sql_return_maparray("SHOW DATA FROM ${name}")
            if (data.any { it.IndexName == name && it.RowCount.toString() == reported.toString() }) {
                break
            }
            assertTrue(System.nanoTime() < deadline, "Index row count for ${name} was not reported: ${data}")
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
        for (def histTbl : ["fixed_block_analyze_dup", "fixed_block_analyze_comp_agg", "fixed_block_analyze_random_agg"]) {
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
    } finally {
        sql "DROP TABLE IF EXISTS internal.__internal_schema.histogram_statistics"
        original.each { name, value -> sql "SET GLOBAL ${name} = ${value}" }
    }
}
