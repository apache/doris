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

suite("inner_join_x") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "set enable_sync_mv_cost_based_rewrite=false"


    // =======================  test table with aggregate key ============================
    sql """
    drop table if  exists t1;
    """

    sql """
    CREATE TABLE IF NOT EXISTS t1 (
                k int,
                a int,
                int_value int sum,
                char_value char(10) max,
                date_value date max
            )
            ENGINE=OLAP
            aggregate KEY(k,a)
            DISTRIBUTED BY HASH(k) BUCKETS 2 properties("replication_num" = "1")

    """

    def mv_name="v_t1"
    createMV ( """
     create materialized view ${mv_name} as select k%2 as kk,a, sum(int_value), max(date_value) from t1 group by kk, a;
    """)

    sql """
    insert into t1 values
    (1,1,1,'a', '2020-12-01'),
    (2,2,2,'b', '2021-12-01'),
    (3,2,2,'c', '2022-12-01'),
    (4,2,4,'c', '2023-12-01');
    """

    def query =  """
    select a from t1
    """
    
    explain {
        sql("${query}")
        notContains("${mv_name}(${mv_name})")
    }

    order_qt_query_before "${query}"
    

    sql """ DROP MATERIALIZED VIEW IF EXISTS  ${mv_name} on t1"""

    order_qt_query_after "${query}"

    sql """
    drop table if exists t1 
    """

    // =======================  test table with duplicate key ============================
    sql """
    drop table if  exists t1;
    """

    sql """
    CREATE TABLE IF NOT EXISTS t1 (
                k int,
                a int,
                int_value int,
                char_value char(10),
                date_value date
            )
            ENGINE=OLAP
            duplicate KEY(k,a)
            DISTRIBUTED BY HASH(k) BUCKETS 2 properties("replication_num" = "1")

    """

    mv_name="v_t1"
    createMV ( """
     create materialized view ${mv_name} as select k%2 as kk,a, sum(int_value), max(date_value) from t1 group by kk, a;
    """)

    sql """
    insert into t1 values
    (1,1,1,'a', '2020-12-01'),
    (2,2,2,'b', '2021-12-01'),
    (3,2,2,'c', '2022-12-01'),
    (4,2,4,'c', '2023-12-01');
    """

    query =  """
    select a from t1
    """
    
    explain {
        sql("${query}")
        notContains("t1(${mv_name})")
    }

    order_qt_query_before "${query}"
    

    sql """ DROP MATERIALIZED VIEW IF EXISTS  ${mv_name} on t1"""

    order_qt_query_after "${query}"

    sql """
    drop table if exists t1 
    """
}
