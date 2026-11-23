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
    def table = "fixed_block_analyze_dup"
    def variables = ["enable_fixed_block_stats_sampling", "statistics_sample_unit_rows", "enable_auto_analyze"]
    def original = variables.collectEntries { name ->
        [(name): sql("SHOW GLOBAL VARIABLES LIKE '${name}'")[0][1]]
    }
    def waitForRows = { name ->
        def deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(10)
        while (true) {
            def data = sql_return_maparray("SHOW DATA FROM ${name}")
            if (data.any { it.IndexName == name && it.RowCount.toString() == "8192" }) {
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
        sql "SET GLOBAL enable_auto_analyze = false"
        sql "SET GLOBAL enable_fixed_block_stats_sampling = true"
        sql "SET GLOBAL statistics_sample_unit_rows = 8"
        sql "DROP TABLE IF EXISTS ${table}"
        sql """CREATE TABLE ${table} (k INT NOT NULL, bucket INT, v VARCHAR(32), missing INT, wide STRING)
               DUPLICATE KEY(k, bucket)
               PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (4096), PARTITION p2 VALUES LESS THAN (8192))
               DISTRIBUTED BY HASH(bucket) BUCKETS 4 PROPERTIES ("replication_num"="1")"""
        sql """INSERT INTO ${table} SELECT number, number % 17, IF(number % 4 = 0, NULL, 'a:;b'), NULL,
               REPEAT('x', 128) FROM numbers("number"="8192")"""
        waitForRows(table)
        setFeConfigTemporary([statistics_max_string_column_length: 64]) {
            sql "ANALYZE TABLE ${table} WITH SAMPLE ROWS 1024 WITH SYNC"
            def rows = sql "SHOW COLUMN STATS ${table}"
            def byName = rows.collectEntries { [(it[0]): it] }
            assertEquals(["k", "bucket", "v", "missing"] as Set, byName.keySet())
            checkCounts(rows, ["k", "v", "missing"])
            assertEquals(byName.k[2], byName.k[3])
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
            def keys = agg ? "AGGREGATE KEY(k, v)" : "UNIQUE KEY(k)"
            def props = agg ? "" : """, "enable_unique_key_merge_on_write"="${mode == 'mow'}" """
            sql "DROP TABLE IF EXISTS ${name}"
            sql """CREATE TABLE ${name} (k INT NOT NULL, v INT NOT NULL) ${keys}
                   DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ("replication_num"="1" ${props})"""
            sql """INSERT INTO ${name} SELECT number, number % 8 FROM numbers("number"="8192")"""
            waitForRows(name)
            if (mode == "mow") {
                sql "DELETE FROM ${name} WHERE k < 32"
                assertEquals(8160L, sql("SELECT COUNT(1) FROM ${name}")[0][0] as long)
            }
            sql "ANALYZE TABLE ${name} WITH SAMPLE ROWS 1024 WITH SYNC"
            def rows = sql "SHOW COLUMN STATS ${name}"
            checkCounts(rows, mode == "mow" ? ["k", "v"] : [])
            def key = rows.find { it[0] == "k" }
            assertNotNull(key)
            if (!agg) {
                assertEquals(key[2], key[3])
            }
            if (mode == "mow") {
                def persisted = { stats -> stats.collectEntries { [(it[0]): [it[2..5], it[7..8], it[17]]] } }
                def previous = persisted(rows)
                sql "DELETE FROM ${name} WHERE k >= 0"
                assertEquals(0L, sql("SELECT COUNT(1) FROM ${name}")[0][0] as long)
                sql "ANALYZE TABLE ${name} WITH SAMPLE ROWS 1024 WITH SYNC"
                assertEquals(previous, persisted(sql("SHOW COLUMN STATS ${name}")))
            }
        }
    } finally {
        original.each { name, value -> sql "SET GLOBAL ${name} = ${value}" }
    }
}
