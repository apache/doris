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
This suite is a one dimensional test case file.
 */
suite("partition_mv_rewrite_dimension_1") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_1
    """

    sql """CREATE TABLE `orders_1` (
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
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_1
    """

    sql """CREATE TABLE `lineitem_1` (
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
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_1 values 
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
    insert into lineitem_1 values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """alter table orders_1 modify column o_comment set stats ('row_count'='10');"""
    sql """alter table lineitem_1 modify column l_comment set stats ('row_count'='7');"""

    sql """analyze table orders_1 with sync;"""
    sql """analyze table lineitem_1 with sync;"""

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
    def mv_name_1 = "mv_join_1"
    def join_direction_mv_1 = """
        select l_Shipdate, o_Orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        """

    create_async_mv(db, mv_name_1, join_direction_mv_1)
    def job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(job_name_1)

    def join_direction_sql_1 = """
        select L_SHIPDATE 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        """
    def join_direction_sql_2 = """
        select L_SHIPDATE 
        from  orders_1 
        left join lineitem_1 
        on orders_1.o_orderkey = lineitem_1.L_ORDERKEY
        """
    mv_rewrite_success(join_direction_sql_1, mv_name_1)
    compare_res(join_direction_sql_1 + " order by 1")
    mv_rewrite_fail(join_direction_sql_2, mv_name_1)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1};"""


    def mv_name_2 = "mv_join_2"
    def join_direction_mv_2 = """
        select L_SHIPDATE, O_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        inner join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        """

    create_async_mv(db, mv_name_2, join_direction_mv_2)
    def job_name_2 = getJobName(db, mv_name_2)
    waitingMTMVTaskFinished(job_name_2)

    def join_direction_sql_3 = """
        select l_shipdaTe 
        from lineitem_1 
        inner join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        """
    def join_direction_sql_4 = """
        select L_shipdate 
        from  orders_1 
        inner join lineitem_1 
        on orders_1.o_orderkey = lineitem_1.l_orderkey
        """
    mv_rewrite_success(join_direction_sql_3, mv_name_2)
    compare_res(join_direction_sql_3 + " order by 1")
    mv_rewrite_success(join_direction_sql_4, mv_name_2)
    compare_res(join_direction_sql_4 + " order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_2};"""

    // join filter position
    def join_filter_stmt_1 = """
        select L_SHIPDATE, o_orderdate, l_partkey, l_suppkey, O_orderkey 
        from lineitem_1  
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey"""
    def join_filter_stmt_2 = """
        select l_shipdate, o_orderdate, L_partkey, l_suppkey, O_ORDERKEY    
        from (select * from lineitem_1 where l_shipdate = '2023-10-17' ) t1 
        left join orders_1 
        on t1.l_orderkey = orders_1.o_orderkey"""
    def join_filter_stmt_3 = """
        select l_shipdate, o_orderdate, l_Partkey, l_suppkey, o_orderkey  
        from lineitem_1 
        left join (select * from orders_1 where o_orderdate = '2023-10-17' ) t2 
        on lineitem_1.l_orderkey = t2.o_orderkey"""
    def join_filter_stmt_4 = """
        select l_shipdate, o_orderdate, l_parTkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where l_shipdate = '2023-10-17' and o_orderdate = '2023-10-17'"""
    def join_filter_stmt_5 = """
        select l_shipdate, o_orderdate, l_partkeY, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where l_shipdate = '2023-10-17'"""
    def join_filter_stmt_6 = """
        select l_shipdatE, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  o_orderdate = '2023-10-17'"""
    def join_filter_stmt_7 = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkeY 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.O_ORDERKEY=1"""

    def mv_list = [
            join_filter_stmt_1, join_filter_stmt_2, join_filter_stmt_3, join_filter_stmt_4,
            join_filter_stmt_5, join_filter_stmt_6, join_filter_stmt_7]

    for (int i =0; i < mv_list.size(); i++) {
        logger.info("i:" + i)
        def join_filter_mv = """join_filter_mv_${i}"""
        create_async_mv(db, join_filter_mv, mv_list[i])
        def job_name = getJobName(db, join_filter_mv)
        waitingMTMVTaskFinished(job_name)
        def res_1 = sql """show partitions from ${join_filter_mv};"""
        logger.info("res_1:" + res_1)
        if (i == 0) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 2) {
                    continue
                }
                mv_rewrite_success(mv_list[j], join_filter_mv)
                compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
            }
        } else if (i == 1) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 1 || j == 4 || j == 3) {
                    mv_rewrite_success(mv_list[j], join_filter_mv)
                    compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                } else {
                    mv_rewrite_fail(mv_list[j], join_filter_mv)
                }
            }
        } else if (i == 2) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 2 || j == 3 || j == 5) {
                    mv_rewrite_success(mv_list[j], join_filter_mv)
                    compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                } else {
                    mv_rewrite_fail(mv_list[j], join_filter_mv)
                }

            }
        } else if (i == 3) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 3) {
                    mv_rewrite_success(mv_list[j], join_filter_mv)
                    compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                } else {
                    mv_rewrite_fail(mv_list[j], join_filter_mv)
                }
            }
        } else if (i == 4) {
            for (int j = 0; j < mv_list.size(); j++) {
                logger.info("j:" + j)
                if (j == 4 || j == 1 || j == 3) {
                    mv_rewrite_success(mv_list[j], join_filter_mv)
                    compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                } else {
                    mv_rewrite_fail(mv_list[j], join_filter_mv)
                }
            }
        } else if (i == 5) {
            for (int j = 0; j < mv_list.size(); j++) {
                if (j == 5 || j == 3) {
                    mv_rewrite_success(mv_list[j], join_filter_mv)
                    compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                } else {
                    mv_rewrite_fail(mv_list[j], join_filter_mv)
                }

            }
        } else if (i == 6) {
            for (int j = 0; j < mv_list.size(); j++) {
                if (j == 6) {
                    mv_rewrite_success(mv_list[j], join_filter_mv)
                    compare_res(mv_list[j] + " order by 1, 2, 3, 4, 5")
                } else {
                    mv_rewrite_fail(mv_list[j], join_filter_mv)
                }

            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${join_filter_mv};"""
    }

    // join type
    def join_type_stmt_1 = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.L_ORDERKEY = orders_1.o_orderkey"""
    def join_type_stmt_2 = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey  
        from lineitem_1 
        inner join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey"""

    // Todo: right/cross/full/semi/anti join
    // Currently, only left join and inner join are supported.
    def join_type_stmt_3 = """
        select l_shipdate, o_orderdatE, l_partkey, l_suppkey
        from lineitem_1
        right join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey"""
//    def join_type_stmt_4 = """
//        select l_shipdate, o_orderdate, l_partkey, l_suppkey
//        from lineitem_1
//        cross join orders_1"""
    def join_type_stmt_5 = """
        select l_shipdate, o_orderdate, L_partkey, l_suppkey
        from lineitem_1
        full join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey"""
    def join_type_stmt_6 = """
        select l_shipdate, l_partkey, l_suppkey, l_Shipmode, l_orderkey 
        from lineitem_1
        left semi join orders_1
        on lineitem_1.L_ORDERKEY = orders_1.o_orderkey"""
    def join_type_stmt_7 = """
        select o_orderkey, o_custkey, o_Orderdate, o_clerk, o_totalprice 
        from lineitem_1
        right semi join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey"""
    def join_type_stmt_8 = """
        select l_shipdate, l_partkey, l_suppkeY, l_shipmode, l_orderkey 
        from lineitem_1
        left anti join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkeY"""
    def join_type_stmt_9 = """
        select o_orderkey, o_custkeY, o_orderdate, o_clerk, o_totalprice 
        from lineitem_1
        right anti join orders_1
        on lineitem_1.L_ORDERKEY = orders_1.o_orderkey"""
    def join_type_stmt_list = [join_type_stmt_1, join_type_stmt_2, join_type_stmt_3,
                               join_type_stmt_5, join_type_stmt_6, join_type_stmt_7, join_type_stmt_8, join_type_stmt_9]
    for (int i = 0; i < join_type_stmt_list.size(); i++) {
        logger.info("i:" + i)
        String join_type_mv = """join_type_mv_${i}"""
        if (i in [2, 5, 7]) {
            create_async_mv(db, join_type_mv, join_type_stmt_list[i])
        } else if (i == 3) {
            create_async_mv(db, join_type_mv, join_type_stmt_list[i])
        } else {
            create_async_mv(db, join_type_mv, join_type_stmt_list[i])
        }
        def job_name = getJobName(db, join_type_mv)
        waitingMTMVTaskFinished(job_name)
        for (int j = 0; j < join_type_stmt_list.size(); j++) {
            logger.info("j:" + j)
            if (i == j) {
                mv_rewrite_success(join_type_stmt_list[j], join_type_mv)
                compare_res(join_type_stmt_list[j] + " order by 1,2,3,4")
            } else {
                mv_rewrite_fail(join_type_stmt_list[j], join_type_mv)
            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${join_type_mv};"""
    }

    // agg
    // agg + without group by + with agg function
    def agg_mv_name_1 = "agg_mv_name_1"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1};"""
    sql """DROP TABLE IF EXISTS ${agg_mv_name_1}"""
    create_async_mv(db, agg_mv_name_1, """
            select
            sum(O_TOTALPRICE) as sum_total,
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_1
    """)
    def agg_job_name_1 = getJobName(db, agg_mv_name_1)
    waitingMTMVTaskFinished(agg_job_name_1)

    def agg_sql_1 = """select 
        count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1, 
        count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
        sum(O_totalprice), 
        max(o_totalprice), 
        min(o_totalprice), 
        count(*) 
        from orders_1
        """
    mv_rewrite_success(agg_sql_1, agg_mv_name_1)
    compare_res(agg_sql_1 + " order by 1,2,3,4,5,6")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1};"""

    // agg + with group by + without agg function
    def agg_mv_name_2 = "agg_mv_name_2"
    def agg_mv_stmt_2 = """
        select o_orderdatE, O_SHIPPRIORITY, o_comment  
            from orders_1 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment  
        """
    create_async_mv(db, agg_mv_name_2, agg_mv_stmt_2)
    def agg_job_name_2 = getJobName(db, agg_mv_name_2)
    waitingMTMVTaskFinished(agg_job_name_2)
    sql """analyze table ${agg_mv_name_2} with sync;"""

    def agg_sql_2 = """select O_shippriority, o_commenT 
            from orders_1 
            group by 
            o_shippriority, 
            o_comment 
        """
    def agg_sql_explain_2 = sql """explain ${agg_sql_2};"""
    def mv_index_1 = agg_sql_explain_2.toString().indexOf("MaterializedViewRewriteFail:")
    assert(mv_index_1 != -1)
    assert(agg_sql_explain_2.toString().substring(0, mv_index_1).indexOf(agg_mv_name_2) != -1)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_2};"""

    // agg + with group by + with agg function
    def agg_mv_name_3 = "agg_mv_name_3"
    def agg_mv_stmt_3 = """
        select o_orderdatE, o_shippriority, o_comment, 
            sum(o_totalprice) as sum_total, 
            max(o_totalpricE) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_1 
            group by 
            o_orderdatE, 
            o_shippriority, 
            o_comment 
        """
    create_async_mv(db, agg_mv_name_3, agg_mv_stmt_3)
    def agg_job_name_3 = getJobName(db, agg_mv_name_3)
    waitingMTMVTaskFinished(agg_job_name_3)
    sql """analyze table ${agg_mv_name_3} with sync;"""

    def agg_sql_3 = """select o_shipprioritY, o_comment, 
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
            sum(o_totalprice), 
            max(o_totalprice), 
            min(o_totalprice), 
            count(*) 
            from orders_1 
            group by 
            o_shippriority, 
            o_commenT 
        """
    def agg_sql_explain_3 = sql """explain ${agg_sql_3};"""
    def mv_index_2 = agg_sql_explain_3.toString().indexOf("MaterializedViewRewriteFail:")
    assert(mv_index_2 != -1)
    assert(agg_sql_explain_3.toString().substring(0, mv_index_2).indexOf(agg_mv_name_3) != -1)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_3};"""


    // Todo: query partittial rewriting
//    def query_partition_mv_name_1 = "query_partition_mv_name_1"
//    def query_partition_mv_stmt_1 = """
//        select l_shipdate, o_orderdate, l_partkey, l_suppkey, count(*)
//        from lineitem_1
//        left join orders_1
//        on lineitem_1.l_orderkey = orders_1.o_orderkey
//        """
//    create_async_mv(db, query_partition_mv_name_1, query_partition_mv_stmt_1)
//    def query_partition_job_name_1 = getJobName(db, query_partition_mv_name_1)
//    waitingMTMVTaskFinished(query_partition_job_name_1)
//
//    def query_partition_sql_1 = """select l_shipdate, l_partkey, count(*) from lineitem_1;"""
//    def query_partition_sql_2 = """select o_orderdate, count(*) from orders_1;"""
//    explain {
//        sql("${query_partition_sql_1}")
//        contains "${query_partition_mv_name_1}(${query_partition_mv_name_1})"
//    }
//    compare_res(query_partition_sql_1)
//    explain {
//        sql("${query_partition_sql_2}")
//        contains "${query_partition_mv_name_1}(${query_partition_mv_name_1})"
//    }
//    sql """DROP MATERIALIZED VIEW IF EXISTS ${query_partition_mv_name_1};"""

    // view partital rewriting
    def view_partition_mv_name_1 = "view_partition_mv_name_1"
    def view_partition_mv_stmt_1 = """
        select l_shipdatE, l_partkey, l_orderkey from lineitem_1 group by l_shipdate, l_partkey, l_orderkeY"""
    create_async_mv(db, view_partition_mv_name_1, view_partition_mv_stmt_1)
    def view_partition_job_name_1 = getJobName(db, view_partition_mv_name_1)
    waitingMTMVTaskFinished(view_partition_job_name_1)

    def view_partition_sql_1 = """select t.l_shipdate, o_orderdate, t.l_partkey 
        from (select l_shipdate, l_partkey, l_orderkey from lineitem_1 group by l_shipdate, l_partkey, l_orderkey) t
        left join orders_1   
        on t.l_orderkey = orders_1.o_orderkey group by t.l_shipdate, o_orderdate, t.l_partkey
        """
    mv_rewrite_success(view_partition_sql_1, view_partition_mv_name_1)
    compare_res(view_partition_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${view_partition_mv_name_1};"""

    // Todo: union rewrte
//    def union_mv_name_1 = "union_mv_name_1"
//    def union_mv_stmt_1 = """
//        select l_shipdate, o_orderdate, l_partkey, count(*)
//        from lineitem_1
//        left join orders_1
//        on lineitem_1.l_orderkey = orders_1.o_orderkey
//        where l_shipdate >= "2023-12-04"
//        """
//    create_async_mv(db, union_mv_name_1, union_mv_stmt_1)
//    def union_job_name_1 = getJobName(db, union_mv_name_1)
//    waitingMTMVTaskFinished(union_job_name_1)
//
//    def union_sql_1 = """select l_shipdate, o_orderdate, l_partkey, count(*)
//        from lineitem_1
//        left join orders_1
//        on lineitem_1.l_orderkey = orders_1.o_orderkey
//        where l_shipdate >= "2023-12-01"
//        """
//    explain {
//        sql("${union_sql_1}")
//        contains "${union_mv_name_1}(${union_mv_name_1})"
//    }
//    sql """DROP MATERIALIZED VIEW IF EXISTS ${union_mv_name_1};"""

    // predicate compensate
    def predicate_mv_name_1 = "predicate_mv_name_1"
    def predicate_mv_stmt_1 = """
        select l_shipdatE, o_orderdate, l_partkey 
        from lineitem_1 
        left join orders_1   
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate >= "2023-10-17"
        """
    create_async_mv(db, predicate_mv_name_1, predicate_mv_stmt_1)
    def predicate_job_name_1 = getJobName(db, predicate_mv_name_1)
    waitingMTMVTaskFinished(predicate_job_name_1)

    def predicate_sql_1 = """
        select l_shipdate, o_orderdate, l_partkeY 
        from lineitem_1 
        left join orders_1   
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate >= "2023-10-17" and l_partkey = 1
        """
    mv_rewrite_success(predicate_sql_1, predicate_mv_name_1)
    compare_res(predicate_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${predicate_mv_name_1};"""

    // Todo: project rewriting
//    def rewriting_mv_name_1 = "rewriting_mv_name_1"
//    def rewriting_mv_stmt_1 = """
//        select o_orderdate, o_shippriority, o_comment, o_orderkey, o_shippriority + o_custkey,
//        case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
//        case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
//        from orders_1
//        where  o_orderkey > 1 + 1;
//        """
//    create_async_mv(db, rewriting_mv_name_1, rewriting_mv_stmt_1)
//    def rewriting_job_name_1 = getJobName(db, rewriting_mv_name_1)
//    waitingMTMVTaskFinished(rewriting_job_name_1)
//
//    def rewriting_sql_1 = """select o_shippriority, o_comment, o_shippriority + o_custkey  + o_orderkey,
//            case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
//        case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
//            from orders_1
//           where  o_orderkey > (-3) + 5;
//        """
//    explain {
//        sql("${rewriting_sql_1}")
//        contains "${rewriting_mv_name_1}(${rewriting_mv_name_1})"
//    }
//    sql """DROP MATERIALIZED VIEW IF EXISTS ${rewriting_mv_name_1};"""

    // single table
    mv_name_1 = "single_tb_mv_1"
    def single_table_mv_stmt_1 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_1 
        where l_commitdate like '2023-10-%'
        """

    create_async_mv(db, mv_name_1, single_table_mv_stmt_1)
    job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(job_name_1)

    def single_table_query_stmt_1 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_1 
        where l_commitdate like '2023-10-%'
        """
    def single_table_query_stmt_2 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_1 
        where l_commitdate like '2023-10-%' and l_partkey > 0 + 1
        """

    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
    compare_res(single_table_query_stmt_1 + " order by 1,2,3")

    mv_rewrite_success(single_table_query_stmt_2, mv_name_1)
    compare_res(single_table_query_stmt_2 + " order by 1,2,3")


    single_table_mv_stmt_1 = """
        select sum(o_totalprice) as sum_total, 
            max(o_totalpricE) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_1 where o_orderdate >= '2022-10-17' + interval '1' year
        """

    create_async_mv(db, mv_name_1, single_table_mv_stmt_1)
    job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(job_name_1)

    // not support currently
//    single_table_query_stmt_1 = """
//        select sum(o_totalprice) as sum_total,
//            max(o_totalpricE) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_1 where o_orderdate >= '2022-10-17' + interval '1' year
//        """
//    single_table_query_stmt_2 = """
//        select sum(o_totalprice) as sum_total,
//            max(o_totalpricE) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_1 where o_orderdate > '2022-10-17' + interval '1' year
//        """
//    explain {
//        sql("${single_table_query_stmt_1}")
//        contains "${mv_name_1}(${mv_name_1})"
//    }
//    compare_res(single_table_query_stmt_1 + " order by 1,2,3,4")
//    explain {
//        sql("${single_table_query_stmt_2}")
//        contains "${mv_name_1}(${mv_name_1})"
//    }
//    compare_res(single_table_query_stmt_2 + " order by 1,2,3,4")


    single_table_mv_stmt_1 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_1 
        where l_commitdate in (select l_commitdate from lineitem_1) 
        """

    create_async_mv(db, mv_name_1, single_table_mv_stmt_1)
    job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(job_name_1)

    single_table_query_stmt_1 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_1 
        where l_commitdate in (select l_commitdate from lineitem_1) 
        """
    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
    compare_res(single_table_query_stmt_1 + " order by 1,2,3")

// not supported currently
//    single_table_mv_stmt_1 = """
//        select l_Shipdate, l_partkey, l_suppkey
//        from lineitem_1
//        where exists (select l_commitdate from lineitem_1 where l_commitdate like "2023-10-17")
//        """
//
//    create_async_mv(db, mv_name_1, single_table_mv_stmt_1)
//    job_name_1 = getJobName(db, mv_name_1)
//    waitingMTMVTaskFinished(job_name_1)
//
//    single_table_query_stmt_1 = """
//        select l_Shipdate, l_partkey, l_suppkey
//        from lineitem_1
//        where exists (select l_commitdate from lineitem_1 where l_commitdate like "2023-10-17")
//        """
//    explain {
//        sql("${single_table_query_stmt_1}")
//        contains "${mv_name_1}(${mv_name_1})"
//    }
//    compare_res(single_table_query_stmt_1 + " order by 1,2,3")
//
//
//    single_table_mv_stmt_1 = """
//        select t.l_Shipdate, t.l_partkey, t.l_suppkey
//        from (select * from lineitem_1) as t
//        where exists (select l_commitdate from lineitem_1 where l_commitdate like "2023-10-17")
//        """
//
//    create_async_mv(db, mv_name_1, single_table_mv_stmt_1)
//    job_name_1 = getJobName(db, mv_name_1)
//    waitingMTMVTaskFinished(job_name_1)
//
//    single_table_query_stmt_1 = """
//        select t.l_Shipdate, t.l_partkey, t.l_suppkey
//        from (select * from lineitem_1) as t
//        where exists (select l_commitdate from lineitem_1 where l_commitdate like "2023-10-17")
//        """
//    explain {
//        sql("${single_table_query_stmt_1}")
//        contains "${mv_name_1}(${mv_name_1})"
//    }
//    compare_res(single_table_query_stmt_1 + " order by 1,2,3")
}
