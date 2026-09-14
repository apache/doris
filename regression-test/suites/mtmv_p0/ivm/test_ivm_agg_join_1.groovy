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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_agg_join_1") {

    // =========================================================
    // Part 1: SUM + COUNT(*) over 2-table inner join
    // =========================================================
    sql """drop materialized view if exists ivm_aj1_p1_mv;"""
    sql """drop table if exists ivm_aj1_p1_orders;"""
    sql """drop table if exists ivm_aj1_p1_custs;"""

    sql """
        CREATE TABLE ivm_aj1_p1_orders (
            order_id INT,
            cust_id INT,
            amount INT
        )
        UNIQUE KEY(order_id)
        DISTRIBUTED BY HASH(order_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj1_p1_custs (
            cust_id INT,
            cname VARCHAR(32)
        )
        UNIQUE KEY(cust_id)
        DISTRIBUTED BY HASH(cust_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj1_p1_orders VALUES (1,1,100),(2,1,200),(3,2,50);"""
    sql """INSERT INTO ivm_aj1_p1_custs VALUES (1,'Alice'),(2,'Bob');"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj1_p1_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj1_p1_custs.cust_id AS cust_id,
               SUM(ivm_aj1_p1_orders.amount) AS total,
               COUNT(*) AS cnt
        FROM ivm_aj1_p1_orders
        INNER JOIN ivm_aj1_p1_custs
            ON ivm_aj1_p1_orders.cust_id = ivm_aj1_p1_custs.cust_id
        GROUP BY ivm_aj1_p1_custs.cust_id;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p1_mv")
    order_qt_p1_complete """SELECT cust_id, total, cnt FROM ivm_aj1_p1_mv"""

    sql """INSERT INTO ivm_aj1_p1_orders VALUES (4,2,75);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p1_mv")
    order_qt_p1_incr """SELECT cust_id, total, cnt FROM ivm_aj1_p1_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p1_mv")
    order_qt_p1_complete2 """SELECT cust_id, total, cnt FROM ivm_aj1_p1_mv"""

    // =========================================================
    // Part 2: Multiple agg functions (SUM, COUNT, AVG) over inner join
    // =========================================================
    sql """drop materialized view if exists ivm_aj1_p2_mv;"""
    sql """drop table if exists ivm_aj1_p2_dept;"""
    sql """drop table if exists ivm_aj1_p2_emp;"""

    sql """
        CREATE TABLE ivm_aj1_p2_dept (
            did INT,
            dname VARCHAR(32)
        )
        UNIQUE KEY(did)
        DISTRIBUTED BY HASH(did) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj1_p2_emp (
            eid INT,
            did_ref INT,
            salary INT
        )
        UNIQUE KEY(eid)
        DISTRIBUTED BY HASH(eid) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj1_p2_dept VALUES (1,'Eng'),(2,'Sales');"""
    sql """INSERT INTO ivm_aj1_p2_emp VALUES (1,1,100),(2,1,200),(3,2,150);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj1_p2_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj1_p2_dept.dname AS dname,
               COUNT(*) AS headcount,
               SUM(ivm_aj1_p2_emp.salary) AS total_sal,
               AVG(ivm_aj1_p2_emp.salary) AS avg_sal
        FROM ivm_aj1_p2_emp
        INNER JOIN ivm_aj1_p2_dept
            ON ivm_aj1_p2_emp.did_ref = ivm_aj1_p2_dept.did
        GROUP BY ivm_aj1_p2_dept.dname;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p2_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p2_mv")
    order_qt_p2_complete """SELECT dname, headcount, total_sal, avg_sal FROM ivm_aj1_p2_mv"""

    sql """INSERT INTO ivm_aj1_p2_emp VALUES (4,2,250);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p2_mv")
    order_qt_p2_incr """SELECT dname, headcount, total_sal, avg_sal FROM ivm_aj1_p2_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p2_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p2_mv")
    order_qt_p2_complete2 """SELECT dname, headcount, total_sal, avg_sal FROM ivm_aj1_p2_mv"""

    // =========================================================
    // Part 3: GROUP BY on left (delta) table side
    // =========================================================
    sql """drop materialized view if exists ivm_aj1_p3_mv;"""
    sql """drop table if exists ivm_aj1_p3_regions;"""
    sql """drop table if exists ivm_aj1_p3_stores;"""

    sql """
        CREATE TABLE ivm_aj1_p3_regions (
            rid INT,
            rname VARCHAR(32)
        )
        UNIQUE KEY(rid)
        DISTRIBUTED BY HASH(rid) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj1_p3_stores (
            sid INT,
            rid_ref INT,
            revenue INT
        )
        UNIQUE KEY(sid)
        DISTRIBUTED BY HASH(sid) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj1_p3_regions VALUES (1,'East'),(2,'West');"""
    sql """INSERT INTO ivm_aj1_p3_stores VALUES (1,1,1000),(2,1,2000),(3,2,500);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj1_p3_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj1_p3_stores.rid_ref,
               COUNT(*) AS store_cnt,
               SUM(ivm_aj1_p3_stores.revenue) AS total_rev
        FROM ivm_aj1_p3_stores
        INNER JOIN ivm_aj1_p3_regions
            ON ivm_aj1_p3_stores.rid_ref = ivm_aj1_p3_regions.rid
        GROUP BY ivm_aj1_p3_stores.rid_ref;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p3_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p3_mv")
    order_qt_p3_complete """SELECT rid_ref, store_cnt, total_rev FROM ivm_aj1_p3_mv"""

    sql """INSERT INTO ivm_aj1_p3_stores VALUES (4,2,800);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p3_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p3_mv")
    order_qt_p3_incr """SELECT rid_ref, store_cnt, total_rev FROM ivm_aj1_p3_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p3_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p3_mv")
    order_qt_p3_complete2 """SELECT rid_ref, store_cnt, total_rev FROM ivm_aj1_p3_mv"""

    // =========================================================
    // Part 4: GROUP BY columns from both tables
    // =========================================================
    sql """drop materialized view if exists ivm_aj1_p4_mv;"""
    sql """drop table if exists ivm_aj1_p4_t1;"""
    sql """drop table if exists ivm_aj1_p4_t2;"""

    sql """
        CREATE TABLE ivm_aj1_p4_t1 (
            k1 INT,
            cat VARCHAR(32),
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj1_p4_t2 (
            k2 INT,
            k1_ref INT,
            region VARCHAR(32),
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj1_p4_t1 VALUES (1,'A',10),(2,'B',20),(3,'A',30);"""
    sql """INSERT INTO ivm_aj1_p4_t2 VALUES (1,1,'X',100),(2,2,'Y',200),(3,3,'X',300);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj1_p4_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj1_p4_t1.cat,
               ivm_aj1_p4_t2.region,
               COUNT(*) AS cnt,
               SUM(ivm_aj1_p4_t1.v1) AS s1
        FROM ivm_aj1_p4_t1
        INNER JOIN ivm_aj1_p4_t2
            ON ivm_aj1_p4_t1.k1 = ivm_aj1_p4_t2.k1_ref
        GROUP BY ivm_aj1_p4_t1.cat, ivm_aj1_p4_t2.region;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p4_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p4_mv")
    order_qt_p4_complete """SELECT cat, region, cnt, s1 FROM ivm_aj1_p4_mv"""

    sql """INSERT INTO ivm_aj1_p4_t2 VALUES (4,1,'Y',400);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p4_mv")
    order_qt_p4_incr """SELECT cat, region, cnt, s1 FROM ivm_aj1_p4_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p4_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p4_mv")
    order_qt_p4_complete2 """SELECT cat, region, cnt, s1 FROM ivm_aj1_p4_mv"""

    // =========================================================
    // Part 5: CROSS JOIN with scalar aggregation (no GROUP BY)
    // =========================================================
    sql """drop materialized view if exists ivm_aj1_p5_mv;"""
    sql """drop table if exists ivm_aj1_p5_t1;"""
    sql """drop table if exists ivm_aj1_p5_t2;"""

    sql """
        CREATE TABLE ivm_aj1_p5_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj1_p5_t2 (
            k2 INT,
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj1_p5_t1 VALUES (1,10),(2,20);"""
    sql """INSERT INTO ivm_aj1_p5_t2 VALUES (1,100),(2,200);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj1_p5_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT COUNT(*) AS cnt,
               SUM(ivm_aj1_p5_t1.v1 + ivm_aj1_p5_t2.v2) AS total
        FROM ivm_aj1_p5_t1
        CROSS JOIN ivm_aj1_p5_t2;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p5_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p5_mv")
    order_qt_p5_complete """SELECT cnt, total FROM ivm_aj1_p5_mv"""

    sql """INSERT INTO ivm_aj1_p5_t1 VALUES (3,30);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p5_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p5_mv")
    order_qt_p5_incr """SELECT cnt, total FROM ivm_aj1_p5_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj1_p5_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj1_p5_mv")
    order_qt_p5_complete2 """SELECT cnt, total FROM ivm_aj1_p5_mv"""
}
