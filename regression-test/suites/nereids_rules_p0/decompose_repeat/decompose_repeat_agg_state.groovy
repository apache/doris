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

suite("decompose_repeat_agg_state") {
    sql "drop table if exists decompose_repeat_agg_state"
    sql """create table decompose_repeat_agg_state (
        a int, b int, c int, v decimal(18, 2), n int not null, label varchar(20) not null default 'key'
    ) distributed by hash(a) buckets 3 properties('replication_num'='1')"""
    sql """insert into decompose_repeat_agg_state(a,b,c,v,n) values
        (1,1,1,10,1),(1,1,1,20,2),(1,1,2,100,3),
        (1,2,1,null,4),(null,1,1,40,5),(null,1,1,null,6),
        (2,2,2,null,7),(2,2,2,null,8)"""

    // Differential checks use the original execution as the runtime oracle.
    def queries = [
        """select label,b,c,avg(v),count(*),grouping_id(label,b,c)
           from decompose_repeat_agg_state group by rollup(label,b,c)""",
        """select n,b,c,avg(v),count(*),grouping_id(n,b,c)
           from decompose_repeat_agg_state group by rollup(n,b,c)""",
        """select a,b,c,avg(v),sum(v),count(*),count(v),min(v),max(v),sum0(v),
                  grouping(a),grouping_id(a,b,c)
           from decompose_repeat_agg_state group by rollup(a,b,c)""",
        """select a,b,c,avg(v),avg(n),avg(a),count(a),grouping_id(a,b,c)
           from decompose_repeat_agg_state group by cube(a,b,c)""",
        """select a,b,c,avg(v),count(*),grouping_id(a,b,c)
           from decompose_repeat_agg_state
           group by grouping sets ((a),(a,b,c),(a,b),(),())""",
        """select a,b,c,avg(v),count(*),grouping(c),grouping_id(a,b,c)
           from decompose_repeat_agg_state
           group by grouping sets ((a,b,c),(a,b,c),(a,b),(),())""",
        """select avg(v),avg(v)+1,count(*),count(v)
           from decompose_repeat_agg_state group by rollup(a,b,c)""",
        """select c,a,b,grouping(c),avg(v),grouping_id(c,a,b),count(*)
           from decompose_repeat_agg_state group by grouping sets ((a),(a,b,c),(a,b),())""",
        """select a+1,b,c,avg(v+1),avg(v)+sum(v)
           from decompose_repeat_agg_state group by rollup(a+1,b,c) having count(*)>1""",
        """select a,b,c,avg(v),count(*),count(v),sum0(v)
           from decompose_repeat_agg_state where n<0 group by rollup(a,b,c)""",
        """select a,b,c,avg(v),count(*),count(v),sum0(v)
           from decompose_repeat_agg_state where a=2 group by rollup(a,b,c)""",
        """select a,b,c,sum(v),count(*),count(v),min(v),max(v),grouping_id(a,b,c)
           from decompose_repeat_agg_state group by rollup(a,b,c)"""
    ]
    // Exercise state rollup beyond AVG, including multi-argument functions and complex inputs.
    // Round statistical results because regrouping changes floating-point addition order.
    for (grouping in ["rollup(a,b,c)", "cube(a,b,c)",
                      "grouping sets ((a),(a,b,c),(a,b),(),())",
                      "grouping sets ((a,b,c),(a,b,c),(a,b),(),())"]) {
        for (predicate in ["true", "a=2", "n<0"]) {
            queries.add("""select a,b,c,sum(v),count(*),min(v),max(v),
                          round(avg(v),6),round(stddev_samp(v),6),round(stddev_pop(v),6),
                          round(var_samp(v),6),round(var_pop(v),6),
                          round(covar_pop(v,n),6),round(covar_samp(v,n),6),
                          round(avg_weighted(v,n),6),
                          round(percentile(cast(v as double),0.5),6),
                          ndv(v),bitmap_union_count(to_bitmap(n)),
                          grouping_id(a,b,c)
                   from decompose_repeat_agg_state where ${predicate} group by ${grouping}""")
        }
    }
    for (query in queries) {
        sql "set disable_nereids_rules='DECOMPOSE_REPEAT'"
        def baseline = order_sql(query)
        sql "set disable_nereids_rules=''"
        def stateResult = order_sql(query)
        assertEquals(baseline, stateResult)
    }
    explain {
        sql "select a,b,c,avg(v) from decompose_repeat_agg_state group by rollup(a,b,c)"
        contains "avg_combine"
        contains "avg_merge"
        contains "avg_finalize"
    }
    explain {
        sql """select a,b,c,sum(v),count(*),avg(v),stddev_samp(v),ndv(v)
               from decompose_repeat_agg_state group by rollup(a,b,c)"""
        contains "stddev_samp_combine"
        contains "ndv_combine"
        notContains "sum_combine"
        notContains "sum_finalize"
        notContains "count_combine"
        notContains "count_finalize"
    }
}
