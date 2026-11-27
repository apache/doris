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

// FE codec changes must not overlap other suites.
suite("test_nullable_wrapped_combine_sink_codec", "nonConcurrent") {
    context.reconnectToMasterFe()
    sql "set enable_agg_state=true"
    sql "drop table if exists wrapped_combine_input"
    sql "drop table if exists wrapped_combine_sink"
    sql """
        create table wrapped_combine_input (k int not null, v int null)
        duplicate key(k) distributed by hash(k) buckets 1 properties("replication_num"="1")
    """
    // Two rows cover a valid state and a state with no valid input. Reuse them for
    // each wrapper; no larger data set is needed to distinguish outer SQL NULL.
    sql "insert into wrapped_combine_input values (1, 2), (2, null)"
    setFeConfigTemporary(["be_exec_version": 19]) {
        sql """
            create table wrapped_combine_sink (
                k int not null, was_null boolean replace not null, s agg_state<avg(int null)> generic
            )
            aggregate key(k) distributed by hash(k) buckets 1 properties("replication_num"="1")
        """
    }
    setFeConfigTemporary(["be_exec_version": 20]) {
        def expressions = [
            // Keys 11/12 and 21/22: the second group must be an outer SQL NULL.
            "if(k = 1, avg_combine(v), null)",
            "case when k = 1 then avg_combine(v) end",
            // Keys 31/32: group 1 selects the fallback; group 2 selects a non-NULL
            // empty state. COALESCE must not treat that empty state as SQL NULL.
            "coalesce(if(k = 1, null, avg_combine(v)), if(k = 1, avg_combine(v), null))",
            // Keys 41/42: both alternatives are NULL for group 2. The distinct
            // producers prevent the two alternatives from becoming one expression.
            "coalesce(if(k = 1, avg_combine(v), null), " +
                "if(k = 1, avg_combine(cast(v + 1 as int)), null))"
        ]
        expressions.eachWithIndex { expression, index ->
            // Stored AGG_STATE columns are NOT NULL. Record the expression's outer
            // NULL first, then select a fallback state of 7 before writing. An empty
            // state is not SQL NULL and must not select that fallback.
            sql """
                insert into wrapped_combine_sink
                select k + ${(index + 1) * 10}, (${expression}) is null,
                       coalesce(${expression}, avg_combine(nullable(cast(7 as int))))
                from wrapped_combine_input group by k
            """
        }
    }
    for (int version : [19, 20]) {
        setFeConfigTemporary(["be_exec_version": version]) {
            // AVG(2) is 2. Outer NULL selects the fallback state of 7. The non-NULL
            // empty state at key 32 keeps a NULL final value instead of selecting 7.
            quickRunTest("wrapped_values_${version}", """
                select k, was_null, avg_finalize(s)
                from wrapped_combine_sink order by k
            """, true)
        }
    }
}
