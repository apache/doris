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

suite("mv_base_case_test") {

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

    String db = context.config.getDbNameByFile(context.file)
    sql """use ${db}"""

    def mv_name_agg = "agg_mv"
    def sql_stmt_agg = """select t.sum_total from (select 
            sum(o_totalprice) as sum_total  
            from orders_2_agg where o_orderdate >= "2023-10-17" )  as t
            where t.sum_total = 3"""
    mv_rewrite_success(sql_stmt_agg, mv_name_agg)
    compare_res(sql_stmt_agg + " order by 1")

    def mv_name_dup = "dup_mv"
    def sql_stmt_dup = """select
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders_2_3
            """
    mv_rewrite_success(sql_stmt_dup, mv_name_dup)
    compare_res(sql_stmt_dup + " order by 1,2,3,4,5,6")

    def mv_name_uniq = "uniq_mv"
    def sql_stmt_uniq = """select o_orderkey + o_custkey , 
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_2_uniq  
            where o_orderkey > (-3) + 5 
            group by o_orderdate, o_orderkey + o_custkey, case when o_shippriority > 1 then 1 else 2 end
            """
    mv_rewrite_success(sql_stmt_uniq, mv_name_uniq)
    compare_res(sql_stmt_uniq + " order by 1,2,3,4,5")


    sql """
    insert into orders_2_agg values 
    (null, 1, '2023-10-17', 'o', 99.5, 'a', 'b', 1, 'yy'),
    (6, 5, '2023-10-19', 'o', 99.5, 'a', 'b', 1, 'yy');
    """
    sql """analyze table orders_2_agg with sync;"""
    mv_rewrite_success(sql_stmt_agg, mv_name_agg)
    compare_res(sql_stmt_agg + " order by 1")


    sql """
    insert into orders_2_3 values 
    (null, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (6, 5, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');
    """
    sql """analyze table orders_2_3 with sync;"""
    mv_rewrite_success(sql_stmt_dup, mv_name_dup)
    compare_res(sql_stmt_dup + " order by 1,2,3,4,5,6")

    sql """
    insert into orders_2_uniq values 
    (null, 1, '2023-10-17', 'o', 99.5, 'a', 'b', 1, 'yy'),
    (6, 5, '2023-10-19', 'o', 99.5, 'a', 'b', 1, 'yy');
    """
    sql """analyze table orders_2_uniq with sync;"""
    mv_rewrite_success(sql_stmt_uniq, mv_name_uniq)
    compare_res(sql_stmt_uniq + " order by 1,2,3,4,5")


}

