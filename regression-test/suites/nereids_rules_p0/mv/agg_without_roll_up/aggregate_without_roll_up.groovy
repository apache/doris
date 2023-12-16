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

suite("aggregate_without_roll_up") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"

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
    drop table if exists lineitem
    """

    sql"""
    CREATE TABLE IF NOT EXISTS lineitem (
      L_ORDERKEY    INTEGER NOT NULL,
      L_PARTKEY     INTEGER NOT NULL,
      L_SUPPKEY     INTEGER NOT NULL,
      L_LINENUMBER  INTEGER NOT NULL,
      L_QUANTITY    DECIMALV3(15,2) NOT NULL,
      L_EXTENDEDPRICE  DECIMALV3(15,2) NOT NULL,
      L_DISCOUNT    DECIMALV3(15,2) NOT NULL,
      L_TAX         DECIMALV3(15,2) NOT NULL,
      L_RETURNFLAG  CHAR(1) NOT NULL,
      L_LINESTATUS  CHAR(1) NOT NULL,
      L_SHIPDATE    DATE NOT NULL,
      L_COMMITDATE  DATE NOT NULL,
      L_RECEIPTDATE DATE NOT NULL,
      L_SHIPINSTRUCT CHAR(25) NOT NULL,
      L_SHIPMODE     CHAR(10) NOT NULL,
      L_COMMENT      VARCHAR(44) NOT NULL
    )
    DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
    DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    drop table if exists partsupp
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp (
      PS_PARTKEY     INTEGER NOT NULL,
      PS_SUPPKEY     INTEGER NOT NULL,
      PS_AVAILQTY    INTEGER NOT NULL,
      PS_SUPPLYCOST  DECIMALV3(15,2)  NOT NULL,
      PS_COMMENT     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(PS_PARTKEY, PS_SUPPKEY)
    DISTRIBUTED BY HASH(PS_PARTKEY) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    waiteCreateTableFinished("lineitem")
    sql """ insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-11', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');"""

    waiteCreateTableFinished("orders")
    sql """
    insert into orders values
    (1, 1, 'ok', 99.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 2, 'ok', 109.2, '2023-12-09', 'c','d',2, 'mm'),
    (2, 3, 'ok', 109.2, '2023-12-09', 'c','d',2, 'mm');  
    """

    waiteCreateTableFinished("partsupp")
    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    def check_rewrite = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            contains "(${mv_name})"
        }
    }

    // select + from + inner join + group by
    def mv1_0 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY, sum(O_TOTALPRICE) as sum_alias " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "group by lineitem.L_LINENUMBER, orders.O_CUSTKEY "
    def query1_0 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY, sum(O_TOTALPRICE) as sum_alias " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "group by lineitem.L_LINENUMBER, orders.O_CUSTKEY "
    // fix later
//    order_qt_query1_0_before "${query1_0}"
    check_rewrite(mv1_0, query1_0, "mv1_0")
//    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""
}

