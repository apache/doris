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

// Correctness of the query cache incremental merge: when a partition keeps
// receiving hourly loads, a stale cache entry is reused by scanning only the
// delta rowsets since the cached version and merging them with the cached
// partial aggregation blocks. Every query below is checked against the same
// query with the cache disabled, so the suite passes in any environment
// (including those where incremental merge falls back to a full recompute,
// e.g. cloud mode) while exercising the incremental path on local storage.
suite("query_cache_incremental") {
    def tableName = "test_query_cache_incremental"
    def uniqueTableName = "test_query_cache_incremental_mow"
    def querySql = """
        SELECT
            url,
            SUM(cost) AS total_cost,
            COUNT(*) AS cnt
        FROM ${tableName}
        WHERE dt >= '2026-01-01'
          AND dt < '2026-01-15'
        GROUP BY url
    """

    def normalize = { rows ->
        return rows.collect { row -> row.collect { col -> String.valueOf(col) }.join("|") }.sort()
    }

    // Compare the cached query result against the uncached one, twice: the
    // first cached run may fill or incrementally merge the entry, the second
    // one should serve it.
    def checkConsistency = { String sqlText ->
        sql "set enable_query_cache=false"
        def expected = normalize(sql(sqlText))
        sql "set enable_query_cache=true"
        assertEquals(expected, normalize(sql(sqlText)))
        assertEquals(expected, normalize(sql(sqlText)))
    }

    sql "set enable_nereids_distribute_planner=true"
    sql "set enable_query_cache=true"
    sql "set enable_query_cache_incremental=true"
    sql "set parallel_pipeline_task_num=3"
    sql "set enable_sql_cache=false"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            dt DATE,
            user_id INT,
            url STRING,
            cost BIGINT
        )
        ENGINE=OLAP
        DUPLICATE KEY(dt, user_id)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20260101 VALUES LESS THAN ("2026-01-05"),
            PARTITION p20260105 VALUES LESS THAN ("2026-01-10"),
            PARTITION p20260110 VALUES LESS THAN ("2026-01-15")
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        ('2026-01-01',1,'/a',10),
        ('2026-01-01',2,'/b',20),
        ('2026-01-02',3,'/c',30),
        ('2026-01-06',1,'/a',15),
        ('2026-01-07',3,'/c',35),
        ('2026-01-11',1,'/a',50),
        ('2026-01-12',3,'/c',70)
    """

    // The query cache participates in this plan.
    explain {
        sql(querySql)
        contains("DIGEST")
    }

    // Fill the cache, then serve from it.
    checkConsistency(querySql)

    // Simulate the hourly load pattern: only the latest partition receives new
    // data, so the entries of the older partitions stay valid while the hot
    // partition entry is stale and gets incrementally merged. Crossing
    // query_cache_max_incremental_merge_count (BE config, default 8) also
    // exercises the forced full recompute that compacts the entry.
    for (int i = 1; i <= 10; i++) {
        sql """
            INSERT INTO ${tableName} VALUES
            ('2026-01-13',${i},'/a',${i}),
            ('2026-01-13',${100 + i},'/inc',${10 * i})
        """
        checkConsistency(querySql)
    }

    // A delete in the delta cannot be merged into the cached partial result;
    // the query must fall back to a full recompute and stay correct.
    sql "DELETE FROM ${tableName} PARTITION p20260110 WHERE user_id = 1"
    checkConsistency(querySql)

    sql """
        INSERT INTO ${tableName} VALUES
        ('2026-01-14',999,'/after-delete',1)
    """
    checkConsistency(querySql)

    sql "DROP TABLE IF EXISTS ${tableName}"

    // A UNIQUE merge-on-write table takes the incremental path as long as the
    // loads only append new keys (the delete bitmap of the delta window stays
    // empty); a load that rewrites a pre-existing key falls back to one full
    // recompute and re-bases the entry, after which pure appends are
    // incremental again. Results must stay correct in every phase.
    sql "DROP TABLE IF EXISTS ${uniqueTableName}"
    sql """
        CREATE TABLE ${uniqueTableName} (
            dt DATE,
            user_id INT,
            cost BIGINT
        )
        ENGINE=OLAP
        UNIQUE KEY(dt, user_id)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20260101 VALUES LESS THAN ("2026-01-05"),
            PARTITION p20260105 VALUES LESS THAN ("2026-01-10")
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    def uniqueQuerySql = """
        SELECT dt, SUM(cost) AS total_cost
        FROM ${uniqueTableName}
        GROUP BY dt
    """
    sql """
        INSERT INTO ${uniqueTableName} VALUES
        ('2026-01-01',1,10),
        ('2026-01-02',2,20),
        ('2026-01-06',3,30)
    """
    checkConsistency(uniqueQuerySql)
    // The hourly-append pattern on a primary-key table: every load only adds
    // brand-new keys, so the stale entry merges incrementally.
    for (int i = 1; i <= 3; i++) {
        sql "INSERT INTO ${uniqueTableName} VALUES ('2026-01-06',${100 + i},${i})"
        checkConsistency(uniqueQuerySql)
    }
    // A backfill rewrites history through the delete bitmap: user_id 1 gets a
    // new cost, which forces one full recompute that re-bases the entry.
    sql "INSERT INTO ${uniqueTableName} VALUES ('2026-01-01',1,100)"
    checkConsistency(uniqueQuerySql)
    // After the re-base, pure appends take the incremental path again.
    sql "INSERT INTO ${uniqueTableName} VALUES ('2026-01-06',200,7)"
    checkConsistency(uniqueQuerySql)

    sql "DROP TABLE IF EXISTS ${uniqueTableName}"
}
