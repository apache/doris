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
    def expected1 = sql """
    SELECT SUM(amt) FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
    """
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
    def expected3 = sql """
    SELECT SUM(amt) FROM tb_detail WHERE dt >= '2024-02-01' AND dt <= '2024-02-29'
    """
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
    def expected5 = sql """
    SELECT SUM(amt) FROM tb_detail_v2 WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
    """
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

    def query7 = """
    SELECT SUM(amt) AS gmv
    FROM tb_detail_v2
    WHERE dt >= '2025-01-05' AND dt <= '2025-01-31'
    """

    explain {
        sql("${query7}")
        contains("mv_month_v2 fail")
    }
}
