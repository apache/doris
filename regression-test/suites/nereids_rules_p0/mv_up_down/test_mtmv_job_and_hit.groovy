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

suite("test_upgrade_downgrade_compatibility_mtmv","p0,mtmv,restart_fe") {

    String db = context.config.getDbNameByFile(context.file)
    String orders_tb = "up_down_mtmv_orders"
    String lineitem_tb = "up_down_mtmv_lineitem"
    String mtmv_name = "up_down_mtmv_test_mv"

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

    String mtmv_sql = """select l_Shipdate, o_Orderdate, l_partkey, l_suppkey 
        from ${lineitem_tb} 
        left join ${orders_tb} 
        on ${lineitem_tb}.l_orderkey = ${orders_tb}.o_orderkey"""

    def select_count1 = sql """select count(*) from ${mtmv_name}"""
    logger.info("select_count1: " + select_count1)

    explain {
        sql("${mtmv_sql}")
        contains "${mtmv_name}(${mtmv_name})"
    }
    compare_res(mtmv_sql + " order by 1,2,3,4")

    sql """
    insert into ${orders_tb} values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (6, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (6, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (6, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20');
    """

    sql """
    insert into ${lineitem_tb} values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (6, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (6, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (6, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """refresh MATERIALIZED VIEW ${mtmv_name} auto;"""

    // insert and refresh mtmv
    def job_name = getJobName(db, mtmv_name)
    waitingMTMVTaskFinishedByMvName(mtmv_name)
    def select_count2 = sql """select count(*) from ${mtmv_name}"""
    logger.info("select_count2: " + select_count2)
    assertTrue(select_count2[0][0] != select_count1[0][0])

    explain {
        sql("${mtmv_sql}")
        contains "${mtmv_name}(${mtmv_name})"
    }
    compare_res(mtmv_sql + " order by 1,2,3,4")

    // pause
    def job_status = sql """select * from jobs("type"="mv") where Name="${job_name}";"""
    assertTrue(job_status[0][8] == "RUNNING")
    sql """PAUSE MATERIALIZED VIEW JOB ON ${mtmv_name};"""
    job_status = sql """select * from jobs("type"="mv") where Name="${job_name}";"""
    assertTrue(job_status[0][8] == "PAUSED")

    explain {
        sql("${mtmv_sql}")
        contains "${mtmv_name}(${mtmv_name})"
    }
    compare_res(mtmv_sql + " order by 1,2,3,4")

    // resume
    sql """RESUME MATERIALIZED VIEW JOB ON ${mtmv_name};"""
    job_status = sql """select * from jobs("type"="mv") where Name="${job_name}";"""
    assertTrue(job_status[0][8] == "RUNNING")

    explain {
        sql("${mtmv_sql}")
        contains "${mtmv_name}(${mtmv_name})"
    }
    compare_res(mtmv_sql + " order by 1,2,3,4")

    // drop
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mtmv_name};"""
    sql """DROP TABLE IF EXISTS ${mtmv_name}"""
    test {
        sql """select count(*) from ${mtmv_name}"""
        exception "does not exist"
    }

    // create
    sql"""
        CREATE MATERIALIZED VIEW ${mtmv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(l_shipdate) 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1')  
        AS  
        ${mtmv_sql}
        """

    job_name = getJobName(db, mtmv_name)
    waitingMTMVTaskFinishedByMvName(mtmv_name)

    def select_count3 = sql """select count(*) from ${mtmv_name}"""
    logger.info("select_count3: " + select_count3)
    assertTrue(select_count3[0][0] == select_count2[0][0])

    explain {
        sql("${mtmv_sql}")
        contains "${mtmv_name}(${mtmv_name})"
    }
    compare_res(mtmv_sql + " order by 1,2,3,4")
}
