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

suite("mtmv_range_datetime_part_up_rewrite") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_materialized_view_nest_rewrite=true"
    sql "SET enable_materialized_view_union_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    String mv_prefix = "range_datetime_up_union"

    sql """
    drop table if exists lineitem_range_datetime_union
    """

    sql """CREATE TABLE `lineitem_range_datetime_union` (
      `l_orderkey` BIGINT NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_suppkey` INT NULL,
      `l_quantity` DECIMAL(15, 2) NULL,
      `l_extendedprice` DECIMAL(15, 2) NULL,
      `l_discount` DECIMAL(15, 2) NULL,
      `l_tax` DECIMAL(15, 2) NULL,
      `l_returnflag` VARCHAR(1) NULL,
      `l_linestatus` VARCHAR(1) NULL,
      `l_commitdate` DATE NULL,
      `l_receiptdate` DATE NULL,
      `l_shipinstruct` VARCHAR(25) NULL,
      `l_shipmode` VARCHAR(10) NULL,
      `l_comment` VARCHAR(44) NULL,
      `l_shipdate` DATEtime not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    partition by range (`l_shipdate`) (
        partition p1 values [("2023-10-29 00:00:00"), ("2023-10-29 01:00:00")), 
        partition p2 values [("2023-10-29 01:00:00"), ("2023-10-29 02:00:00")), 
        partition p3 values [("2023-10-29 02:00:00"), ("2023-10-29 03:00:00")))
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists orders_range_datetime_union
    """

    sql """CREATE TABLE `orders_range_datetime_union` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATEtime not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    partition by range (`o_orderdate`) (
        partition p1 values [("2023-10-29 00:00:00"), ("2023-10-29 01:00:00")), 
        partition p2 values [("2023-10-29 01:00:00"), ("2023-10-29 02:00:00")), 
        partition p3 values [("2023-10-29 02:00:00"), ("2023-10-29 03:00:00")),
        partition p4 values [("2023-10-29 03:00:00"), ("2023-10-29 04:00:00")),
        partition p5 values [("2023-10-29 04:00:00"), ("2023-10-29 05:00:00")))
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into lineitem_range_datetime_union values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 00:00:00'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29 00:00:00'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-29 02:00:00'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 00:00:00'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29 01:00:00'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-29 02:00:00'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 00:00:00');
    """

    sql """
    insert into orders_range_datetime_union values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-29 00:00:00'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-29 00:00:00'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-29 01:00:00'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-29 03:00:00'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-29 04:00:00'),
    (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-29 04:00:00'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-29 02:00:00'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-29 01:00:00'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-29 00:00:00'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-29 02:00:00'); 
    """

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv1;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv1 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`col1`, 'month')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, l_orderkey from lineitem_range_datetime_union as t1 left join orders_range_datetime_union as t2 on t1.l_orderkey = t2.o_orderkey group by col1, l_shipdate, l_orderkey;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv2;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv2 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`col1`, 'month')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'hour') as col1, l_shipdate, l_orderkey from lineitem_range_datetime_union as t1 left join orders_range_datetime_union as t2 on t1.l_orderkey = t2.o_orderkey group by col1, l_shipdate, l_orderkey;"""

    def sql1 = """select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, l_orderkey from lineitem_range_datetime_union as t1 left join orders_range_datetime_union as t2 on t1.l_orderkey = t2.o_orderkey group by col1, l_shipdate, l_orderkey"""
    def sql2 = """select date_trunc(`l_shipdate`, 'hour') as col1, l_shipdate, l_orderkey from lineitem_range_datetime_union as t1 left join orders_range_datetime_union as t2 on t1.l_orderkey = t2.o_orderkey group by col1, l_shipdate, l_orderkey"""

    def localWaitingMTMVTaskFinished = { def jobName ->
        Thread.sleep(2000);
        String showTasks = "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where JobName = '${jobName}' order by CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(4)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING' || status == 'NULL'))
        if (status != "SUCCESS") {
            logger.info("status is not success")
        }
    }

    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def query_stmt_list = [sql1, sql2]
    def mv_name_list = ["${mv_prefix}_mv1", "${mv_prefix}_mv2"]
    for (int i = 0; i < mv_name_list.size(); i++) {
        def job_name = getJobName(db, mv_name_list[i])
        waitingMTMVTaskFinished(job_name)
        mv_rewrite_success(query_stmt_list[i], mv_name_list[i])
        compare_res(query_stmt_list[i] + " order by 1,2,3")
    }


    sql """alter table lineitem_range_datetime_union add partition p4 values [("2023-11-29 03:00:00"), ("2023-11-29 04:00:00"));"""
    sql """insert into lineitem_range_datetime_union values 
        (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-11-29 03:00:00')"""
    for (int i = 0; i < mv_name_list.size(); i++) {
        mv_rewrite_success(query_stmt_list[i], mv_name_list[i])
        compare_res(query_stmt_list[i] + " order by 1,2,3")
    }

    for (int i = 0; i < mv_name_list.size(); i++) {
        sql """refresh MATERIALIZED VIEW ${mv_name_list[i]} auto;"""
        mv_rewrite_success(query_stmt_list[i], mv_name_list[i])
        compare_res(query_stmt_list[i] + " order by 1,2,3")
    }

    sql """insert into lineitem_range_datetime_union values 
        (3, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-11-29 03:00:00');"""
    for (int i = 0; i < mv_name_list.size(); i++) {
        mv_rewrite_success(query_stmt_list[i], mv_name_list[i])
        compare_res(query_stmt_list[i] + " order by 1,2,3")
    }

    for (int i = 0; i < mv_name_list.size(); i++) {
        sql """refresh MATERIALIZED VIEW ${mv_name_list[i]} auto;"""
        mv_rewrite_success(query_stmt_list[i], mv_name_list[i])
        compare_res(query_stmt_list[i] + " order by 1,2,3")
    }

    sql """ALTER TABLE lineitem_range_datetime_union DROP PARTITION IF EXISTS p4 FORCE"""
    for (int i = 0; i < mv_name_list.size(); i++) {
        mv_rewrite_success(query_stmt_list[i], mv_name_list[i])
        compare_res(query_stmt_list[i] + " order by 1,2,3")
    }

    for (int i = 0; i < mv_name_list.size(); i++) {
        sql """refresh MATERIALIZED VIEW ${mv_name_list[i]} auto;"""
        mv_rewrite_success(query_stmt_list[i], mv_name_list[i])
        compare_res(query_stmt_list[i] + " order by 1,2,3")
    }

}
