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

suite("single_table_without_aggregate") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set enable_materialized_view_rewrite=true"

    sql """
    drop table if exists orders
    """


    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      O_ORDERKEY       INTEGER NOT NULL,
      O_CUSTKEY        INTEGER NOT NULL,
      O_ORDERSTATUS    CHAR(1) NOT NULL,
      O_TOTALPRICE     DECIMALV3(15,2) NOT NULL,
      O_ORDERDATE      DATE NOT NULL,
      O_ORDERPRIORITY  CHAR(15) NOT NULL,  
      O_CLERK          CHAR(15) NOT NULL, 
      O_SHIPPRIORITY   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)
    DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO orders VALUES
    (1, 1, 'F', 34.22, '2023-01-01', 'a', 'b', 100, "abc"),(2, 1, 'T', 66.22, '2023-01-02', 'c', 'd', 200, "def")
    """

    sql "analyze table orders with sync;"
    sql """set enable_stats=false;"""

    def check_rewrite = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON orders"""
        createMV("create materialized view ${mv_name} as ${mv_sql}")
        explain {
            sql("${query_sql}")
            contains "(${mv_name})"
        }
        order_qt_query1_0 "${query_sql}"
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name} ON orders"""
    }

    // select + from
    def mv1_0 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d from orders"
    def query1_0 = "select O_ORDERKEY + 100, abs(O_TOTALPRICE) + 1 as abs_price, date_add(O_ORDERDATE, INTERVAL 2 DAY) " +
            "from orders"
    check_rewrite(mv1_0, query1_0, "mv1_0")
    order_qt_query1_0 "${query1_0}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv1_0 ON orders"""


    def mv1_1 = "select " +
            "O_ORDERKEY + 10, abs(O_TOTALPRICE + 100) + 10, date_add(O_ORDERDATE, INTERVAL 2 DAY) " +
            "from orders"
    def query1_1 = "select O_ORDERKEY + 10, abs(O_TOTALPRICE + 100) + 10, " +
            "day(date_add(O_ORDERDATE, INTERVAL 2 DAY))" +
            "from orders"
    // should support but not, need to fix complex expression
//    check_rewrite(mv1_1, query1_1, "mv1_1")
    order_qt_query1_1 "${query1_1}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv1_1 ON orders"""


    // select + from + order by + limit
    def mv2_0 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders "
    def query2_0 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders " +
            "order by O_ORDERKEY"
    check_rewrite(mv2_0, query2_0, "mv2_0")
    order_qt_query2_0 "${query2_0}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv2_0 ON orders"""


    def mv2_1 = "select O_ORDERKEY + 10, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders "
    def query2_1 = "select O_ORDERKEY + 10, abs(O_TOTALPRICE) + 50, O_ORDERDATE as d " +
            "from orders " +
            "order by O_ORDERKEY + 10"
    check_rewrite(mv2_1, query2_1, "mv2_1")
    order_qt_query2_1 "${query2_1}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv2_1 ON orders"""


    def mv2_2 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders "
    def query2_2 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders " +
            "order by O_ORDERKEY limit 10"
    // should support but not, need to fix limit
    check_rewrite(mv2_2, query2_2, "mv2_2")
    order_qt_query2_2 "${query2_2}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv2_2 ON orders"""


    // select + from + filter
    def mv3_0 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders"
    def query3_0 = "select O_ORDERKEY, abs(O_TOTALPRICE), date_add(O_ORDERDATE, INTERVAL 2 DAY) " +
            "from orders " +
            "where O_ORDERKEY = 1 and abs(O_TOTALPRICE) > 39"
    check_rewrite(mv3_0, query3_0, "mv3_0")
    order_qt_query3_0 "${query3_0}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv3_0 ON orders"""

    def mv3_1 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders " +
            "where abs(O_TOTALPRICE) > 10 and O_ORDERKEY > 1"
    def query3_1 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders " +
            "where abs(O_TOTALPRICE) > 12 and O_ORDERKEY > 1"
    // should support but not, need to fix predicate compensation
    // check_rewrite(mv3_1, query3_1, "mv3_1")
    order_qt_query3_1 "${query3_1}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv3_1 ON orders"""


    // select + from + filter + order by + limit
    def mv4_0 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders "
    def query4_0 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders " +
            "where abs(O_TOTALPRICE) > 10 " +
            "order by O_ORDERKEY"
    check_rewrite(mv4_0, query4_0, "mv4_0")
    order_qt_query4_0 "${query4_0}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv4_0 ON orders"""


    def mv4_1 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders "
    def query4_1 = "select O_ORDERKEY, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders " +
            "where abs(O_TOTALPRICE) > 10 " +
            "order by O_ORDERKEY limit 10"
    // should support but not, need to fix limit
    check_rewrite(mv4_1, query4_1, "mv4_1")
    order_qt_query4_1 "${query4_1}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv4_1 ON orders"""


    // select + from + sub query
    def mv5_0 = "select 1, O_ORDERKEY, abs(O_TOTALPRICE) as abs_price , O_ORDERDATE as d " +
            "from orders sub_query"
    def query5_0 = "select sub_query.O_ORDERKEY, sub_query.abs_price, sub_query.d " +
            "from " +
            "(select 1, O_ORDERKEY, abs(O_TOTALPRICE) as abs_price , O_ORDERDATE as d " +
            "from orders) sub_query"
    // should support but not, need to fix sub query with alias
    // check_rewrite(mv5_0, query5_0, "mv5_0")
    order_qt_query5_0 "${query5_0}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv5_0 ON orders"""

    // select + from + order by limit + sub query
    def mv5_1 = "select O_ORDERKEY + 10, abs(O_TOTALPRICE), O_ORDERDATE as d " +
            "from orders sub_query"
    def query5_1 = "select d from " +
            "(select O_ORDERKEY + 10, abs(O_TOTALPRICE) + 50, O_ORDERDATE as d " +
            "from orders " +
            "order by O_ORDERKEY + 10) sub_query"
    // should support but not, need to fix sub query with alias
    // check_rewrite(mv5_1, query5_1, "mv5_1")
    order_qt_query5_1 "${query5_1}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv5_1 ON orders"""
    

    // select + from + filter + sub query
    def mv5_2 = "select 1, O_ORDERKEY, abs(O_TOTALPRICE) as abs_price , O_ORDERDATE as d " +
            "from orders sub_query "
    def query5_2 = "select sub_query.O_ORDERKEY, sub_query.abs_price, sub_query.d " +
            "from " +
            "(select 1, O_ORDERKEY, abs(O_TOTALPRICE) as abs_price , O_ORDERDATE as d " +
            "from orders) sub_query " +
            "where sub_query.abs_price > 10"
    // should support but not, need to fix sub query
    // check_rewrite(mv5_2, query5_2, "mv5_2")
    order_qt_query5_2 "${query5_2}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv5_2 ON orders"""


    // select + from + filter + order by + limit + sub query
    def mv5_3 = "select 1, O_ORDERKEY, abs(O_TOTALPRICE) as abs_price , O_ORDERDATE as d " +
            "from orders "
    def query5_3 = "select sub_query.O_ORDERKEY, sub_query.abs_price, sub_query.d " +
            "from " +
            "(select 1, O_ORDERKEY, abs(O_TOTALPRICE) as abs_price , O_ORDERDATE as d " +
            "from orders) sub_query " +
            "where sub_query.abs_price > 10 " +
            "order by sub_query.O_ORDERKEY limit 10"
    // should support but not, need to fix sub query
    // check_rewrite(mv5_3, query5_3, "mv5_3")
    order_qt_query5_3 "${query5_3}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv5_3 ON orders"""


    // view = select + from
    def view6_0_name = "view6_0"
    sql """drop view if exists ${view6_0_name}"""
    sql """
         create view ${view6_0_name} as ${query1_0}
    """
    def query6_0 = "SELECT __arithmetic_expr_0, count(*) from " +
            "${view6_0_name} " +
            "group by __arithmetic_expr_0"
    // should support but not
    // check_rewrite(mv1_0, query6_0, "mv6_0")
    // order_qt_query6_0 "${query6_0}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv6_0 ON orders"""

    // view = select + from + order by + limit
    def view6_1_name = "view6_1"
    sql """drop view if exists ${view6_1_name}"""
    sql """
         create view ${view6_1_name} as ${query2_1}
    """
    def query6_1 = "select d, count(*) from " +
            "${view6_1_name} " +
            "group by d"
    // should support but not
    // check_rewrite(mv2_1, query6_1, "mv6_1")
    order_qt_query6_1 "${query6_1}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv6_1 ON orders"""

    // view = select + from + filter
    def view6_2_name = "view6_2"
    sql """drop view if exists ${view6_2_name}"""
    sql """
         create view ${view6_2_name} as ${query3_0}
    """
    def query6_2 = "select O_ORDERKEY, count(*) from " +
            "${view6_2_name} " +
            "group by O_ORDERKEY"
    // should support but not
    check_rewrite(mv3_0, query6_2, "mv6_2")
    order_qt_query6_2 "${query6_2}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv6_2 ON orders"""


    // view = select + from + filter + order by + limit
    def view6_3_name = "view6_3"
    sql """drop view if exists ${view6_3_name}"""
    sql """
         create view ${view6_3_name} as ${query4_1}
    """
    def query6_3 = "select O_ORDERKEY, count(*) from " +
            "${view6_3_name} " +
            "group by O_ORDERKEY"
    check_rewrite(mv4_1, query6_3, "mv6_3")
    order_qt_query6_3 "${query6_3}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv6_3 ON orders"""


    // top cte = select + from
    def cte7_0_name = "cte7_0"
    def cte7_0 = "with ${cte7_0_name} as ( ${query1_0} )"

    def query7_0 = "${cte7_0} SELECT abs_price, count(*) from " +
            "${cte7_0_name} " +
            "group by abs_price"
    // should support but not
    check_rewrite(mv1_0, query7_0, "mv7_0")
    order_qt_query7_0 "${query7_0}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv7_0 ON orders"""

    // top cte = select + from + order by + limit
    def cte7_1_name = "cte7_1"
    def cte7_1 = "with ${cte7_1_name} as ( ${query2_1} )"
    def query7_1 = "${cte7_1} select d, count(*) from " +
            "${cte7_1_name} " +
            "group by d"
    // should support but not
    // check_rewrite(mv2_1, query7_1, "mv7_1")
    order_qt_query7_1 "${query7_1}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv7_1 ON orders"""

    // top cte = select + from + filter
    def cte7_2_name = "cte7_2"
    def cte7_2 = "with ${cte7_2_name} as ( ${query3_0} )"

    def query7_2 = "${cte7_2} select O_ORDERKEY, count(*) from " +
            "${cte7_2_name} " +
            "group by O_ORDERKEY"
    // should support but not
    check_rewrite(mv3_0, query7_2, "mv7_2")
    order_qt_query7_2 "${query7_2}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv7_2 ON orders"""


    // top cte = select + from + filter + order by + limit
    def cte7_3_name = "cte7_3"
    def cte7_3 = "with ${cte7_3_name} as ( ${query4_1} )"

    def query7_3 = "${cte7_3} select O_ORDERKEY, count(*) from " +
            "${cte7_3_name} " +
            "group by O_ORDERKEY"
    // should support but not
    check_rewrite(mv4_1, query7_3, "mv7_3")
    order_qt_query7_3 "${query7_3}"
    sql """DROP MATERIALIZED VIEW IF EXISTS mv7_3 ON orders"""
}