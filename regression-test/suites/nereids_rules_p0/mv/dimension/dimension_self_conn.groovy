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

/*
This suite test self connection case
 */
suite("partition_mv_rewrite_dimension_self_conn") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_self_conn
    """

    sql """CREATE TABLE `orders_self_conn` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_self_conn
    """

    sql """CREATE TABLE `lineitem_self_conn` (
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
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_self_conn values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_self_conn values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders_self_conn with sync;"""
    sql """analyze table lineitem_self_conn with sync;"""

    def create_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
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


    // join direction
    def mv_name_1 = "mv_self_conn"

    def join_direction_mv_1 = """
        select t1.l_Shipdate, t2.l_partkey, t1.l_suppkey 
        from lineitem_self_conn as t1 
        left join lineitem_self_conn as t2 
        on t1.l_orderkey = t2.l_orderkey
        """

    create_mv(mv_name_1, join_direction_mv_1)
    def job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(job_name_1)

    def join_direction_sql_1 = """
        select t1.L_SHIPDATE 
        from lineitem_self_conn as t1 
        left join lineitem_self_conn as t2 
        on t1.l_orderkey = t2.l_orderkey
        """
    explain {
        sql("${join_direction_sql_1}")
        contains "${mv_name_1}(${mv_name_1})"
    }
    compare_res(join_direction_sql_1 + " order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1};"""

    // join filter position
    def join_filter_stmt_1 = """select t1.L_SHIPDATE, t2.l_partkey, t1.l_suppkey  
        from lineitem_self_conn as t1 
        left join lineitem_self_conn as t2 
        on t1.l_orderkey = t2.l_orderkey"""
    def join_filter_stmt_2 = """select t1.l_shipdate, t2.L_partkey, t1.l_suppkey     
        from (select * from lineitem_self_conn where l_shipdate = '2023-10-17' ) t1 
        left join lineitem_self_conn as t2 
        on t1.l_orderkey = t2.l_orderkey"""
    def join_filter_stmt_3 = """select t1.l_shipdate, t2.l_Partkey, t1.l_suppkey   
        from lineitem_self_conn as t1 
        left join (select * from lineitem_self_conn where l_shipdate = '2023-10-17' ) t2 
        on t1.l_orderkey = t2.l_orderkey"""
    def join_filter_stmt_4 = """select t1.l_shipdate, t2.l_parTkey, t1.l_suppkey  
        from lineitem_self_conn as t1
        left join lineitem_self_conn as t2 
        on t1.l_orderkey = t2.l_orderkey 
        where t1.l_shipdate = '2023-10-17'"""
    def join_filter_stmt_5 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey  
        from lineitem_self_conn as t1 
        left join lineitem_self_conn as t2 
        on t1.l_orderkey = t2.l_orderkey 
        where t1.l_suppkey=1"""

    def mv_list = [
            join_filter_stmt_1, join_filter_stmt_2, join_filter_stmt_3, join_filter_stmt_4, join_filter_stmt_5]
    def join_self_conn_order = " order by 1, 2, 3"
    for (int i =0; i < mv_list.size(); i++) {
        logger.info("i:" + i)
        def join_self_conn_mv = """join_self_conn_mv_${i}"""
        create_mv(join_self_conn_mv, mv_list[i])
        def job_name = getJobName(db, join_self_conn_mv)
        waitingMTMVTaskFinished(job_name)
        if (i == 0) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 2) {
                    continue
                }
                explain {
                    sql("${mv_list[j]}")
                    contains "${join_self_conn_mv}(${join_self_conn_mv})"
                }
                compare_res(mv_list[j] + join_self_conn_order)
            }
        } else if (i == 1) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 1 || j == 3) {
                    explain {
                        sql("${mv_list[j]}")
                        contains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                    compare_res(mv_list[j] + join_self_conn_order)
                } else {
                    explain {
                        sql("${mv_list[j]}")
                        notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                }
            }
        } else if (i == 2) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 2) {
                    explain {
                        sql("${mv_list[j]}")
                        contains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                    compare_res(mv_list[j] + join_self_conn_order)
                } else {
                    explain {
                        sql("${mv_list[j]}")
                        notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                }

            }
        } else if (i == 3) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 1 || j == 3) {
                    explain {
                        sql("${mv_list[j]}")
                        contains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                    compare_res(mv_list[j] + join_self_conn_order)
                } else {
                    explain {
                        sql("${mv_list[j]}")
                        notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                }
            }
        } else if (i == 4) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 4) {
                    explain {
                        sql("${mv_list[j]}")
                        contains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                    compare_res(mv_list[j] + join_self_conn_order)
                } else {
                    explain {
                        sql("${mv_list[j]}")
                        notContains "${join_self_conn_mv}(${join_self_conn_mv})"
                    }
                }
            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${join_self_conn_mv};"""
    }

    // join type
    def join_type_stmt_1 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
        from lineitem_self_conn as t1 
        left join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_2 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
        from lineitem_self_conn as t1 
        inner join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_3 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
        from lineitem_self_conn as t1 
        right join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_5 = """select t1.l_shipdate, t2.l_partkey, t1.l_suppkey 
        from lineitem_self_conn as t1 
        full join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_6 = """select t1.l_shipdate, t1.l_partkey, t1.l_suppkey 
        from lineitem_self_conn as t1 
        left semi join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_7 = """select t2.l_shipdate, t2.l_partkey, t2.l_suppkey 
        from lineitem_self_conn as t1 
        right semi join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_8 = """select t1.l_shipdate, t1.l_partkey, t1.l_suppkey 
        from lineitem_self_conn as t1 
        left anti join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_9 = """select t2.l_shipdate, t2.l_partkey, t2.l_suppkey 
        from lineitem_self_conn as t1 
        right anti join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY"""
    def join_type_stmt_list = [join_type_stmt_1, join_type_stmt_2, join_type_stmt_3,
                               join_type_stmt_5, join_type_stmt_6, join_type_stmt_7, join_type_stmt_8, join_type_stmt_9]
    for (int i = 0; i < join_type_stmt_list.size(); i++) {
        logger.info("i:" + i)
        String join_type_self_conn_mv = """join_type_self_conn_mv_${i}"""
        create_mv(join_type_self_conn_mv, join_type_stmt_list[i])
        def job_name = getJobName(db, join_type_self_conn_mv)
        waitingMTMVTaskFinished(job_name)
        if (i in [4, 5]) {
            for (int j = 0; j < join_type_stmt_list.size(); j++) {
                logger.info("j: " + j)
                if (j in [4, 5]) {
                    explain {
                        sql("${join_type_stmt_list[j]}")
                        contains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                    }
                    compare_res(join_type_stmt_list[j] + " order by 1,2,3")
                } else {
                    explain {
                        sql("${join_type_stmt_list[j]}")
                        notContains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                    }
                }
            }
        } else if (i in [6, 7]) {
            for (int j = 0; j < join_type_stmt_list.size(); j++) {
                logger.info("j: " + j)
                if (j in [6, 7]) {
                    explain {
                        sql("${join_type_stmt_list[j]}")
                        contains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                    }
                    compare_res(join_type_stmt_list[j] + " order by 1,2,3")
                } else {
                    explain {
                        sql("${join_type_stmt_list[j]}")
                        notContains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                    }
                }
            }
        } else {
            for (int j = 0; j < join_type_stmt_list.size(); j++) {
                logger.info("j:" + j)
                if (i == j) {
                    explain {
                        sql("${join_type_stmt_list[j]}")
                        contains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                    }
                    compare_res(join_type_stmt_list[j] + " order by 1,2,3")
                } else {
                    explain {
                        sql("${join_type_stmt_list[j]}")
                        notContains "${join_type_self_conn_mv}(${join_type_self_conn_mv})"
                    }
                }
            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${join_type_self_conn_mv};"""
    }

    // agg
    // agg + without group by + with agg function
    agg_mv_stmt = """
        select t2.o_orderkey, 
        sum(t1.O_TOTALPRICE) as sum_total,
        max(t1.o_totalprice) as max_total, 
        min(t1.o_totalprice) as min_total, 
        count(*) as count_all, 
        bitmap_union(to_bitmap(case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end)) cnt_1, 
        bitmap_union(to_bitmap(case when t1.o_shippriority > 2 and t1.o_orderkey IN (2) then t1.o_custkey else null end)) as cnt_2 
        from orders_self_conn as t1 
        left join orders_self_conn as t2
        on t1.o_orderkey = t2.o_orderkey
        group by t2.o_orderkey
        """

    create_mv(mv_name_1, agg_mv_stmt)
    job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(job_name_1)

    def agg_sql_1 = """select t2.o_orderkey,
        count(distinct case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end) as cnt_1, 
        count(distinct case when t1.O_SHIPPRIORITY > 2 and t1.o_orderkey IN (2) then t1.o_custkey else null end) as cnt_2, 
        sum(t1.O_totalprice), 
        max(t1.o_totalprice), 
        min(t1.o_totalprice), 
        count(*) 
        from orders_self_conn as t1
        left join orders_self_conn as t2
        on t1.o_orderkey = t2.o_orderkey
        group by t2.o_orderkey
        """
    explain {
        sql("${agg_sql_1}")
        contains "${mv_name_1}(${mv_name_1})"
    }
    compare_res(agg_sql_1 + " order by 1,2,3,4,5,6,7")

    agg_sql_1 = """select t2.o_orderkey,
        count(distinct case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end) as cnt_1, 
        count(distinct case when t1.O_SHIPPRIORITY > 2 and t1.o_orderkey IN (2) then t1.o_custkey else null end) as cnt_2, 
        sum(t1.O_totalprice), 
        max(t1.o_totalprice), 
        min(t1.o_totalprice), 
        count(*) 
        from orders_self_conn as t1
        left join orders_self_conn as t2
        on t1.o_orderkey = t2.o_orderkey
        group by t2.o_orderkey
        """
    explain {
        sql("${agg_sql_1}")
        contains "${mv_name_1}(${mv_name_1})"
    }
    compare_res(agg_sql_1 + " order by 1,2,3,4,5,6")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1};"""

    // agg + with group by + without agg function

    def agg_mv_stmt_2 = """
        select t1.o_orderdatE, t2.O_SHIPPRIORITY, t1.o_comment  
        from orders_self_conn as t1 
        inner join  orders_self_conn as t2 
        on t1.o_orderkey = t2.o_orderkey 
        group by 
        t1.o_orderdate, 
        t2.o_shippriority, 
        t1.o_comment  
        """
    create_mv(mv_name_1, agg_mv_stmt_2)
    def agg_job_name_2 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(agg_job_name_2)
    sql """analyze table ${mv_name_1} with sync;"""

    def agg_sql_2 = """
        select t2.O_SHIPPRIORITY, t1.o_comment  
        from orders_self_conn as t1 
        inner join  orders_self_conn as t2 
        on t1.o_orderkey = t2.o_orderkey 
        group by 
        t2.o_shippriority, 
        t1.o_comment  
        """
    explain {
        sql("${agg_sql_2}")
        contains "${mv_name_1}(${mv_name_1})"
    }
    compare_res(agg_sql_2 + " order by 1,2")

    // agg + with group by + with agg function
    def agg_mv_stmt_3 = """
        select t1.o_orderdatE, t2.o_shippriority, t1.o_comment, 
        sum(t1.o_totalprice) as sum_total, 
        max(t2.o_totalpricE) as max_total, 
        min(t1.o_totalprice) as min_total, 
        count(*) as count_all, 
        bitmap_union(to_bitmap(case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end)) cnt_1, 
        bitmap_union(to_bitmap(case when t2.o_shippriority > 2 and t2.o_orderkey IN (2) then t2.o_custkey else null end)) as cnt_2 
        from orders_self_conn  as t1 
        inner join orders_self_conn as t2
        on t1.o_orderkey = t2.o_orderkey
        group by 
        t1.o_orderdatE, 
        t2.o_shippriority, 
        t1.o_comment 
        """
    create_mv(mv_name_1, agg_mv_stmt_3)
    def agg_job_name_3 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(agg_job_name_3)
    sql """analyze table ${mv_name_1} with sync;"""

    def agg_sql_3 = """
        select t2.o_shippriority, t1.o_comment, 
        sum(t1.o_totalprice) as sum_total, 
        max(t2.o_totalpricE) as max_total, 
        min(t1.o_totalprice) as min_total, 
        count(*) as count_all, 
        bitmap_union(to_bitmap(case when t1.o_shippriority > 1 and t1.o_orderkey IN (1, 3) then t1.o_custkey else null end)) cnt_1, 
        bitmap_union(to_bitmap(case when t2.o_shippriority > 2 and t2.o_orderkey IN (2) then t2.o_custkey else null end)) as cnt_2 
        from orders_self_conn  as t1 
        inner join orders_self_conn as t2
        on t1.o_orderkey = t2.o_orderkey
        group by 
        t2.o_shippriority, 
        t1.o_comment
        """
    explain {
        sql("${agg_sql_3}")
        contains "${mv_name_1}(${mv_name_1})"
    }
    compare_res(agg_sql_3 + " order by 1,2,3,4,5,6")


    // view partital rewriting
    def view_partition_mv_stmt_1 = """
        select t1.l_shipdatE, t2.l_partkey, t1.l_orderkey 
        from lineitem_self_conn as t1 
        inner join lineitem_self_conn as t2 
        on t1.L_ORDERKEY = t2.L_ORDERKEY
        group by t1.l_shipdate, t2.l_partkey, t1.l_orderkeY"""
    create_mv(mv_name_1, view_partition_mv_stmt_1)
    def view_partition_job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(view_partition_job_name_1)

    def view_partition_sql_1 = """select t.l_shipdate, lineitem_self_conn.l_orderkey, t.l_partkey 
        from (
            select t1.l_shipdatE as l_shipdatE, t2.l_partkey as l_partkey, t1.l_orderkey as l_orderkey 
            from lineitem_self_conn as t1 
            inner join lineitem_self_conn as t2 
            on t1.L_ORDERKEY = t2.L_ORDERKEY
            group by t1.l_shipdate, t2.l_partkey, t1.l_orderkeY
        ) t 
        inner join lineitem_self_conn    
        on t.l_partkey = lineitem_self_conn.l_partkey 
        group by t.l_shipdate, lineitem_self_conn.l_orderkey, t.l_partkey
        """
    explain {
        sql("${view_partition_sql_1}")
        contains "${mv_name_1}(${mv_name_1})"
    }
    compare_res(view_partition_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1};"""


    // predicate compensate
    def predicate_mv_stmt_1 = """
        select t1.l_shipdatE, t2.l_shipdate, t1.l_partkey 
        from lineitem_self_conn as t1 
        inner join lineitem_self_conn as t2  
        on t1.l_orderkey = t2.l_orderkey
        where t1.l_shipdate >= "2023-10-17"
        """
    create_mv(mv_name_1, predicate_mv_stmt_1)
    def predicate_job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(predicate_job_name_1)

    def predicate_sql_1 = """
        select t1.l_shipdatE, t2.l_shipdate, t1.l_partkey 
        from lineitem_self_conn as t1 
        inner join lineitem_self_conn as t2  
        on t1.l_orderkey = t2.l_orderkey
        where t1.l_shipdate >= "2023-10-17" and t1.l_partkey = 1
        """
    explain {
        sql("${predicate_sql_1}")
        contains "${mv_name_1}(${mv_name_1})"
    }
    compare_res(predicate_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1};"""

}
