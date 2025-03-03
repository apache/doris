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

suite("mtmv_range_date_part_up") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    String mv_prefix = "range_date_up"

    sql """
    drop table if exists lineitem_range_date
    """

    sql """CREATE TABLE `lineitem_range_date` (
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
      `l_shipdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    partition by range (`l_shipdate`) (
        partition p1 values [("2023-10-29"), ("2023-10-30")), 
        partition p2 values [("2023-10-30"), ("2023-10-31")), 
        partition p3 values [("2023-10-31"), ("2023-11-01")))
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv1;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv1 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(col1) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select l_shipdate as col1 from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv2_1;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv2_1 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(col1) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'day') as col1 from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv2_2;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv2_2 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(l_shipdate) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv3;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv3 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`l_shipdate`, 'day')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select l_shipdate from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv4_1;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv4_1 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`l_shipdate`, 'day')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate  from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv4_2;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv4_2 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`col1`, 'day')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'day') as col1  from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv5;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv5 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(col1) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'month') as col1 from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv6;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv6 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`l_shipdate`, 'month')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select l_shipdate from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv7_1;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv7_1 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`col1`, 'year')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'month') as col1 from lineitem_range_date;"""

    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv7_2;"""
    sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv7_2 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`l_shipdate`, 'year')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'month') as col1, l_shipdate from lineitem_range_date;"""

    // don't create
    sql """DROP MATERIALIZED VIEW if exists ${mv_prefix}_mv8;"""
    try {
        sql """CREATE MATERIALIZED VIEW ${mv_prefix}_mv8 BUILD IMMEDIATE REFRESH AUTO ON MANUAL partition by(date_trunc(`col1`, 'month')) DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') AS  
        select date_trunc(`l_shipdate`, 'year') as col1, l_shipdate from lineitem_range_date;"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Unable to find a suitable base table for partitioning"))
    }

    sql """
    insert into lineitem_range_date values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-31'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-30'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-31'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29');
    """

    def get_part = { def mv_name ->
        def part_res = sql """show partitions from ${mv_name}"""
        return part_res.size()
    }

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

    def mv_name_list = ["${mv_prefix}_mv1", "${mv_prefix}_mv2_1", "${mv_prefix}_mv2_2", "${mv_prefix}_mv3", "${mv_prefix}_mv4_1", "${mv_prefix}_mv4_2", "${mv_prefix}_mv5", "${mv_prefix}_mv6", "${mv_prefix}_mv7_1", "${mv_prefix}_mv7_2"]
    def mv_part = [3, 3, 3, 3, 3, 3, 1, 1, 1, 1]
    for (int i = 0; i < mv_name_list.size(); i++) {
        sql """refresh MATERIALIZED VIEW ${mv_name_list[i]} auto;"""
        def job_name = getJobName(db, mv_name_list[i])
        waitingMTMVTaskFinished(job_name)
        assertEquals(get_part(mv_name_list[i]), mv_part[i])
    }

    sql """alter table lineitem_range_date add partition p4 values [("2023-11-01"), ("2023-11-02"));"""
    sql """insert into lineitem_range_date values 
        (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-11-01')"""

    mv_part = [4, 4, 4, 4, 4, 4, 2, 2, 1, 1]
    for (int i = 0; i < mv_name_list.size(); i++) {
        sql """refresh MATERIALIZED VIEW ${mv_name_list[i]} auto;"""
        def job_name = getJobName(db, mv_name_list[i])
        waitingMTMVTaskFinished(job_name)
        assertEquals(get_part(mv_name_list[i]), mv_part[i])
    }

    sql """alter table lineitem_range_date add partition p5 values [("2023-11-02"), ("2023-12-02"));"""
    sql """insert into lineitem_range_date values 
        (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-11-02')"""

    mv_part = [5, -1, 5, -1, -1, -1, -1, -1, 1, 1]
    for (int i = 0; i < mv_name_list.size(); i++) {
        sql """refresh MATERIALIZED VIEW ${mv_name_list[i]} auto;"""
        def job_name = getJobName(db, mv_name_list[i])
        if (i in [1, 3, 4, 5, 6, 7]) {
            localWaitingMTMVTaskFinished(job_name)
            def mv_task = sql "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where JobName = '${job_name}' order by CreateTime DESC"
            logger.info("mv_task: " + mv_task)
            assertEquals("FAILED", mv_task[0][4])
        } else {
            waitingMTMVTaskFinished(job_name)
            assertEquals(get_part(mv_name_list[i]), mv_part[i])
        }
    }

    sql """alter table lineitem_range_date add partition p6 values [("2023-12-02"), ("2024-12-02"));"""
    sql """insert into lineitem_range_date values 
        (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2024-12-01')"""

    mv_part = [6, -1, 6, -1, -1, -1, -1, -1, -1, -1]
    for (int i = 0; i < mv_name_list.size(); i++) {
        sql """refresh MATERIALIZED VIEW ${mv_name_list[i]} auto;"""
        if (i in [1, 3, 4, 5, 6, 7, 8, 9]) {
            def job_name = getJobName(db, mv_name_list[i])
            localWaitingMTMVTaskFinished(job_name)
            def mv_task = sql "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where JobName = '${job_name}' order by CreateTime DESC"
            assertEquals("FAILED", mv_task[0][4])
        } else {
            def job_name = getJobName(db, mv_name_list[i])
            waitingMTMVTaskFinished(job_name)
            assertEquals(get_part(mv_name_list[i]), mv_part[i])
        }
    }

}
