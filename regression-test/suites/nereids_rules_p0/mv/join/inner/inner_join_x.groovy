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

    sql """
     create materialized view v_t1 as select k%2 as kk,a, sum(int_value), max(date_value) from t1 group by kk, a;
    """

    sql """
    insert into t1 values
    (1,1,1,'a', '2020-12-01'),
    (2,2,2,'b', '2021-12-01'),
    (3,2,2,'c', '2022-12-01'),
    (4,2,4,'c', '2023-12-01');
    """

    sql """
    drop table if  exists t2;
    """

    sql """
    CREATE TABLE IF NOT EXISTS t2 (
                k int,
                a int
            )
            ENGINE=OLAP
            duplicate KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 2 properties("replication_num" = "1")
    """

    sql """
    insert into t2 values(1,2),(2,2);
    """

    sleep(2000)

    def query =  """
    select min(t1.date_value) from t1 inner join t2 on t1.a=t2.a group by t2.k;
    """

    order_qt_query_before "${query}"
    

    sql """ DROP MATERIALIZED VIEW IF EXISTS v_t1 on t1"""

    order_qt_query_after "${query}"

    sql """
    drop table if exists t1 
    """

    sql """
    drop table if exists t2 
    """
}
