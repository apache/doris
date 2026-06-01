package mv.date_trunc
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

suite("test_date_trunc_whole_month_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    // Helper to compute expected results without MV rewrite
    def getExpectedResult = { query ->
        sql "SET enable_materialized_view_rewrite=false"
        def result = sql "${query}"
        sql "SET enable_materialized_view_rewrite=true"
        return result
    }

    sql """
    drop table if exists tb_detail
    """

    sql """
    CREATE TABLE tb_detail (
        dt DATE NOT NULL,
        uuid VARCHAR(50) NOT NULL,
        amt DECIMAL(10, 2) NOT NULL
    ) DUPLICATE KEY(dt, uuid)
    AUTO PARTITION BY RANGE (date_trunc(dt, 'day')) ()
    DISTRIBUTED BY HASH(uuid) BUCKETS 3
    PROPERTIES ("replication_num" = "1")
    """

    sql """
    insert into tb_detail values
    ('2025-01-01', 'uuid1', 100.00),
    ('2025-01-15', 'uuid2', 200.00),
    ('2025-01-31', 'uuid3', 300.00),
    ('2025-02-01', 'uuid4', 400.00),
    ('2025-02-28', 'uuid5', 500.00),
    ('2024-02-01', 'uuid6', 600.00),
    ('2024-02-29', 'uuid7', 700.00)
    """

    sql """analyze table tb_detail with sync"""

    // Create MV with date_trunc by month
    def mv_name = "mv_month"
    sql """
    drop materialized view if exists ${mv_name}
    """

    sql """
    CREATE MATERIALIZED VIEW ${mv_name}
    BUILD IMMEDIATE
    REFRESH ON MANUAL
    PARTITION BY (month_dt)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES ('replication_num' = '1')
    AS SELECT
        date_trunc(dt, 'month') AS month_dt,
        SUM(amt) AS gmv,
        COUNT(DISTINCT uuid) AS uv
    FROM tb_detail
    GROUP BY month_dt
    """

    def job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    sql """analyze table ${mv_name} with sync"""

    // Test 1: Whole month range with <= (January 2025)
    def query1 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail
    WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
    """

    explain {
        sql("${query1}")
        contains("${mv_name} chose")
    }

    def result1 = sql "${query1}"
    def expected1 = getExpectedResult(query1)
    assertEquals(expected1, result1)

    // Test 2: Whole month range with < (February 2025)
    def query2 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail
    WHERE dt >= '2025-02-01' AND dt < '2025-03-01'
    """

    explain {
        sql("${query2}")
        contains("${mv_name} chose")
    }

    def result2 = sql "${query2}"
    def expected2 = getExpectedResult(query2)
    assertEquals(expected2, result2)

    // Test 3: Leap year February (2024)
    def query3 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail
    WHERE dt >= '2024-02-01' AND dt <= '2024-02-29'
    """

    explain {
        sql("${query3}")
        contains("${mv_name} chose")
    }

    def result3 = sql "${query3}"
    def expected3 = getExpectedResult(query3)
    assertEquals(expected3, result3)

    // Test 4: Partial month should NOT rewrite
    def query4 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail
    WHERE dt >= '2025-01-05' AND dt <= '2025-01-31'
    """

    // This should not use MV (partial month)
    explain {
        sql("${query4}")
        contains("${mv_name} fail")
    }

    sql """
    drop table if exists tb_detail_v2
    """

    sql """
    CREATE TABLE tb_detail_v2 (
        dt DATEV2 NOT NULL,
        uuid VARCHAR(50) NOT NULL,
        amt DECIMAL(10, 2) NOT NULL
    ) DUPLICATE KEY(dt, uuid)
    AUTO PARTITION BY RANGE (date_trunc(dt, 'day')) ()
    DISTRIBUTED BY HASH(uuid) BUCKETS 3
    PROPERTIES ("replication_num" = "1")
    """

    sql """
    insert into tb_detail_v2 values
    ('2025-01-01', 'uuid1', 100.00),
    ('2025-01-15', 'uuid2', 200.00),
    ('2025-01-31', 'uuid3', 300.00),
    ('2025-02-01', 'uuid4', 400.00),
    ('2025-02-28', 'uuid5', 500.00),
    ('2024-02-01', 'uuid6', 600.00),
    ('2024-02-29', 'uuid7', 700.00)
    """

    sql """analyze table tb_detail_v2 with sync"""

    sql """
    drop materialized view if exists mv_month_v2
    """

    sql """
    CREATE MATERIALIZED VIEW mv_month_v2
    BUILD IMMEDIATE
    REFRESH ON MANUAL
    PARTITION BY (month_dt)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES ('replication_num' = '1')
    AS SELECT
        date_trunc(dt, 'month') AS month_dt,
        SUM(amt) AS gmv,
        COUNT(DISTINCT uuid) AS uv
    FROM tb_detail_v2
    GROUP BY month_dt
    """

    waitingMTMVTaskFinished(getJobName(db, "mv_month_v2"))

    sql """analyze table mv_month_v2 with sync"""

    def query5 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail_v2
    WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
    """

    explain {
        sql("${query5}")
        contains("mv_month_v2 chose")
    }

    def result5 = sql "${query5}"
    def expected5 = getExpectedResult(query5)
    assertEquals(expected5, result5)

    def query6 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail_v2
    WHERE dt >= '2025-02-01' AND dt < '2025-03-01'
    """

    explain {
        sql("${query6}")
        contains("mv_month_v2 chose")
    }

    def result6 = sql "${query6}"
    def expected6 = getExpectedResult(query6)
    assertEquals(expected6, result6)

    def query7 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail_v2
    WHERE dt >= '2025-01-05' AND dt <= '2025-01-31'
    """

    explain {
        sql("${query7}")
        contains("mv_month_v2 fail")
    }

    // Test 8: DATETIME type should NOT use whole-bucket rewrite
    // Reason: dt <= '2025-01-31 00:00:00' does not cover the full day of Jan 31
    sql """
    drop table if exists tb_detail_datetime
    """

    sql """
    CREATE TABLE tb_detail_datetime (
        dt DATETIME NOT NULL,
        uuid VARCHAR(50) NOT NULL,
        amt DECIMAL(10, 2) NOT NULL
    ) DUPLICATE KEY(dt, uuid)
    AUTO PARTITION BY RANGE (date_trunc(dt, 'day')) ()
    DISTRIBUTED BY HASH(uuid) BUCKETS 3
    PROPERTIES ("replication_num" = "1")
    """

    sql """
    insert into tb_detail_datetime values
    ('2025-01-01 00:00:00', 'uuid1', 100.00),
    ('2025-01-15 12:30:00', 'uuid2', 200.00),
    ('2025-01-31 23:59:59', 'uuid3', 300.00),
    ('2025-02-01 00:00:00', 'uuid4', 400.00)
    """

    sql """analyze table tb_detail_datetime with sync"""

    sql """
    drop materialized view if exists mv_month_datetime
    """

    sql """
    CREATE MATERIALIZED VIEW mv_month_datetime
    BUILD IMMEDIATE
    REFRESH ON MANUAL
    PARTITION BY (month_dt)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES ('replication_num' = '1')
    AS SELECT
        date_trunc(dt, 'month') AS month_dt,
        SUM(amt) AS gmv,
        COUNT(DISTINCT uuid) AS uv
    FROM tb_detail_datetime
    GROUP BY month_dt
    """

    waitingMTMVTaskFinished(getJobName(db, "mv_month_datetime"))

    sql """analyze table mv_month_datetime with sync"""

    // This query looks like a whole month but is NOT because of DATETIME semantics
    // dt <= '2025-01-31 00:00:00' excludes most of Jan 31
    def query8 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail_datetime
    WHERE dt >= '2025-01-01 00:00:00' AND dt <= '2025-01-31 00:00:00'
    """

    // Should NOT use MV because DATETIME boundary semantics differ from DATE
    explain {
        sql("${query8}")
        contains("mv_month_datetime fail")
    }

    // Test 9: DATETIMEV2 type should also NOT use whole-bucket rewrite
    sql """
    drop table if exists tb_detail_datetimev2
    """

    sql """
    CREATE TABLE tb_detail_datetimev2 (
        dt DATETIMEV2 NOT NULL,
        uuid VARCHAR(50) NOT NULL,
        amt DECIMAL(10, 2) NOT NULL
    ) DUPLICATE KEY(dt, uuid)
    AUTO PARTITION BY RANGE (date_trunc(dt, 'day')) ()
    DISTRIBUTED BY HASH(uuid) BUCKETS 3
    PROPERTIES ("replication_num" = "1")
    """

    sql """
    insert into tb_detail_datetimev2 values
    ('2025-01-01 00:00:00', 'uuid1', 100.00),
    ('2025-01-15 12:30:00', 'uuid2', 200.00),
    ('2025-01-31 23:59:59', 'uuid3', 300.00)
    """

    sql """analyze table tb_detail_datetimev2 with sync"""

    sql """
    drop materialized view if exists mv_month_datetimev2
    """

    sql """
    CREATE MATERIALIZED VIEW mv_month_datetimev2
    BUILD IMMEDIATE
    REFRESH ON MANUAL
    PARTITION BY (month_dt)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES ('replication_num' = '1')
    AS SELECT
        date_trunc(dt, 'month') AS month_dt,
        SUM(amt) AS gmv,
        COUNT(DISTINCT uuid) AS uv
    FROM tb_detail_datetimev2
    GROUP BY month_dt
    """

    waitingMTMVTaskFinished(getJobName(db, "mv_month_datetimev2"))

    sql """analyze table mv_month_datetimev2 with sync"""

    def query9 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail_datetimev2
    WHERE dt >= '2025-01-01 00:00:00' AND dt <= '2025-01-31 00:00:00'
    """

    // Should NOT use MV
    explain {
        sql("${query9}")
        contains("mv_month_datetimev2 fail")
    }

    // ===== Test: Multi date_trunc dimension rewrite =====
    sql """
    drop table if exists tb_multi_date
    """

    sql """
    CREATE TABLE tb_multi_date (
        order_dt DATE NOT NULL,
        ship_dt DATE NOT NULL,
        uuid VARCHAR(50) NOT NULL,
        amt DECIMAL(10, 2) NOT NULL
    ) DUPLICATE KEY(order_dt, ship_dt, uuid)
    AUTO PARTITION BY RANGE (date_trunc(order_dt, 'day')) ()
    DISTRIBUTED BY HASH(uuid) BUCKETS 3
    PROPERTIES ("replication_num" = "1")
    """

    sql """
    insert into tb_multi_date values
    ('2025-01-01', '2025-02-01', 'uuid1', 100.00),
    ('2025-01-15', '2025-02-15', 'uuid2', 200.00),
    ('2025-01-31', '2025-02-28', 'uuid3', 300.00)
    """

    sql """analyze table tb_multi_date with sync"""

    sql """
    drop materialized view if exists mv_multi_date
    """

    sql """
    CREATE MATERIALIZED VIEW mv_multi_date
    BUILD IMMEDIATE
    REFRESH ON MANUAL
    PARTITION BY (order_month)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES ('replication_num' = '1')
    AS SELECT
        date_trunc(order_dt, 'month') AS order_month,
        date_trunc(ship_dt, 'month') AS ship_month,
        SUM(amt) AS gmv,
        COUNT(DISTINCT uuid) AS uv
    FROM tb_multi_date
    GROUP BY order_month, ship_month
    """

    waitingMTMVTaskFinished(getJobName(db, "mv_multi_date"))
    sql """analyze table mv_multi_date with sync"""

    // Both date columns have whole-month ranges — should rewrite using MV
    def query_multi = """
    SELECT SUM(amt) AS gmv
    FROM tb_multi_date
    WHERE order_dt >= '2025-01-01' AND order_dt <= '2025-01-31'
      AND ship_dt >= '2025-02-01' AND ship_dt <= '2025-02-28'
    """

    explain {
        sql("${query_multi}")
        contains("mv_multi_date chose")
    }

    def result_multi = sql "${query_multi}"
    def expected_multi = getExpectedResult(query_multi)
    assertEquals(expected_multi, result_multi)

    // One whole-month + one partial range — MV cannot answer because
    // ship_dt is aggregated at month granularity, partial range is not satisfiable
    def query_mixed = """
    SELECT SUM(amt) AS gmv
    FROM tb_multi_date
    WHERE order_dt >= '2025-01-01' AND order_dt <= '2025-01-31'
      AND ship_dt >= '2025-02-10' AND ship_dt <= '2025-02-20'
    """

    explain {
        sql("${query_mixed}")
        contains("mv_multi_date fail")
    }

    // ===== Negative tests: whole-bucket synthesis must not produce wrong results =====

    // Multi-bucket range (3 months): whole-bucket synthesis cannot express this as a single
    // date_trunc equality. The legacy date_trunc path also fails for `<=` upper bounds.
    def query_multi_bucket = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail
    WHERE dt >= '2025-01-01' AND dt <= '2025-03-31'
    """
    explain {
        sql("${query_multi_bucket}")
        contains("${mv_name} fail")
    }
    def result_multi_bucket = sql "${query_multi_bucket}"
    def expected_multi_bucket = getExpectedResult(query_multi_bucket)
    assertEquals(expected_multi_bucket, result_multi_bucket)

    // Single-sided range: only one predicate per slot, so synthesis is skipped (size != 2).
    // The legacy date_trunc path then handles `>=` with a bound aligned to a month boundary.
    // Verifies the new optimization does not interfere with the legacy fallback.
    def query_single_sided = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail
    WHERE dt >= '2025-01-01'
    """
    explain {
        sql("${query_single_sided}")
        contains("${mv_name} chose")
    }
    def result_single_sided = sql "${query_single_sided}"
    def expected_single_sided = getExpectedResult(query_single_sided)
    assertEquals(expected_single_sided, result_single_sided)

    // Row-level filter (dt != '2025-01-15') on a slot aggregated at month granularity.
    // Synthesis fails on size != 2, and the MV cannot satisfy row-level filtering.
    def query_three_predicates = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail
    WHERE dt >= '2025-01-01' AND dt <= '2025-01-31' AND dt != '2025-01-15'
    """
    explain {
        sql("${query_three_predicates}")
        contains("${mv_name} fail")
    }
    def result_three = sql "${query_three_predicates}"
    def expected_three = getExpectedResult(query_three_predicates)
    assertEquals(expected_three, result_three)
}
