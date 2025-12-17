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

suite("decompose_repeat") {
//    sql "set disable_nereids_rules='DECOMPOSE_REPEAT';"
    sql "drop table if exists t1;"
    sql "create table t1(a int, b int, c int, d int) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into t1 values(1,2,3,4),(1,2,3,3),(1,2,1,1),(1,3,2,2);"
    order_qt_sum "select a,b,c,sum(d) from t1 group by rollup(a,b,c);"
    order_qt_agg_func_gby_key_same_col "select a,b,c,d,sum(d) from t1 group by rollup(a,b,c,d);"
    order_qt_multi_agg_func "select a,b,c,sum(d),sum(c),max(a) from t1 group by rollup(a,b,c,d);"
    order_qt_nest_rewrite """
    select a,b,c,c1 from (
    select a,b,c,d,sum(d) c1 from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c))
    ) t group by rollup(a,b,c,c1);
    """
    order_qt_upper_ref """
    select c1+10,a,b,c from (select a,b,c,sum(d) c1 from t1 group by rollup(a,b,c)) t group by c1+10,a,b,c;
    """
    order_qt_another_cte """
        with cte1 as (select 1 as c1 union all select 2)
        select c1 from (
        select c1,1 c2, 2 c3 from cte1 union select c1, 2,3 from cte1
        ) t
        group by rollup(c1,c2,c3);
    """

    // negative case
    order_qt_grouping_func "select a,b,c,d,sum(d),grouping_id(a) from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c))"
    order_qt_avg "select a,b,c,d,avg(d) from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c));"
    order_qt_distinct "select a,b,c,d,sum(distinct d) from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c));"
    order_qt_less_equal_than_3 "select a,b,c,d,sum(distinct d) from t1 group by grouping sets((a,b,c),(a,b,c,d),());"
}