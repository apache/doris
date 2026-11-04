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

suite("test_decimal_aggregate_state_signatures") {
    sql "set enable_agg_state=true"
    sql "drop table if exists decimal_aggregate_state_input"
    sql """create table decimal_aggregate_state_input (k int, v decimal(18,2))
        duplicate key(k) distributed by hash(k) buckets 3
        properties("replication_num"="1")"""
    sql """insert into decimal_aggregate_state_input values
        (1,10),(1,20),(1,20),(2,-3.25),(2,7.50),(3,null)"""
    def functions = ["sum", "sum0", "avg", "multi_distinct_sum", "multi_distinct_sum0", "sum_map", "avg_map"]
    for (boolean enabled : [false, true]) {
        sql "set enable_decimal256=${enabled}"
        def suffix = enabled ? "256" : "128"
        for (String fn : functions) {
            def arg = fn.endsWith("_map") ? "map(cast(1 as int),v)" : "v"
            "order_qt_${fn}_${suffix}_direct"("""select ${fn}(${arg}),${fn}_merge(${fn}_state(${arg}))
                from decimal_aggregate_state_input""")
            "order_qt_${fn}_${suffix}_grouped"("""select k,${fn}(${arg}),${fn}_merge(${fn}_state(${arg}))
                from decimal_aggregate_state_input group by k""")
            "order_qt_${fn}_${suffix}_empty"("""select ${fn}(${arg}),${fn}_merge(${fn}_state(${arg}))
                from decimal_aggregate_state_input where k<0""")
            "order_qt_${fn}_${suffix}_combine"("""select k,${fn}_finalize(s) from
                (select k,${fn}_combine(${arg}) s from decimal_aggregate_state_input group by k) t""")
            "order_qt_${fn}_${suffix}_union"("""select ${fn}_finalize(s) from
                (select ${fn}_union(${fn}_state(${arg})) s from decimal_aggregate_state_input) t""")
            for (int precision : [9, 38]) {
                def wideArg = fn.endsWith("_map") ? "map(cast(1 as int),cast(v as decimal(${precision},6)))"
                        : "cast(v as decimal(${precision},6))"
                "order_qt_${fn}_${suffix}_p${precision}"("""select ${fn}(${wideArg}),
                    ${fn}_merge(${fn}_state(${wideArg})) from decimal_aggregate_state_input""")
            }
        }
        sql "drop table if exists decimal_aggregate_state_stored"
        def columns = functions.collect { fn ->
            def inputType = fn.endsWith("_map") ? "map<int,decimal(18,2)> not null" : "decimal(18,2)"
            "s_${fn} agg_state<${fn}(${inputType})> generic"
        }.join(",")
        sql """create table decimal_aggregate_state_stored (k int,${columns})
            aggregate key(k) distributed by hash(k) buckets 3 properties("replication_num"="1")"""
        def states = functions.collect { fn ->
            def arg = fn.endsWith("_map") ? "map(cast(1 as int),v)" : "v"
            "${fn}_state(${arg})"
        }.join(",")
        sql "insert into decimal_aggregate_state_stored select k,${states} from decimal_aggregate_state_input"
        // A second rowset also exercises storage-side aggregation of persisted states.
        sql "insert into decimal_aggregate_state_stored select k,${states} from decimal_aggregate_state_input"
        sql "set enable_decimal256=${!enabled}"
        for (String fn : functions) {
            "order_qt_${fn}_${suffix}_stored"("select k,${fn}_finalize(s_${fn}) from decimal_aggregate_state_stored")
            "order_qt_${fn}_${suffix}_stored_merge"("select ${fn}_merge(s_${fn}) from decimal_aggregate_state_stored")
            "order_qt_${fn}_${suffix}_stored_union"("""select ${fn}_finalize(s) from
                (select ${fn}_union(s_${fn}) s from decimal_aggregate_state_stored) t""")
        }
        sql "drop table if exists decimal_aggregate_state_recreated"
        def savedDdl = sql "show create table decimal_aggregate_state_stored"
        // SHOW CREATE omits result precision; use the original session to reproduce the signature.
        sql "set enable_decimal256=${enabled}"
        sql savedDdl[0][1].replace("decimal_aggregate_state_stored", "decimal_aggregate_state_recreated")
        sql "set enable_decimal256=${!enabled}"
        sql "insert into decimal_aggregate_state_recreated select * from decimal_aggregate_state_stored"
        for (String fn : functions) {
            "order_qt_${fn}_${suffix}_recreated"("select ${fn}_merge(s_${fn}) from decimal_aggregate_state_recreated")
        }
        test {
            sql "insert into decimal_aggregate_state_stored select k,${states} from decimal_aggregate_state_input"
            exception "exact AggState type match"
        }
    }
    sql "set enable_decimal256=true"
    test {
        sql """select sum_merge(cast(s_sum as agg_state<sum(decimal(76,6))>))
            from decimal_aggregate_state_stored"""
        exception "exact AggState type match"
    }
    // Accumulation exceeds Decimal128; the nested accumulator must actually use Decimal256.
    for (String fn : functions) {
        def arg = fn.endsWith("_map") ? "map(cast(1 as int),v)" : "v"
        "order_qt_${fn}_wide"("""select ${fn}(${arg}),${fn}_merge(${fn}_state(${arg})) from
            (select cast(cast('99999999999999999999999999999999999990' as decimal(38,0)) + k as decimal(38,0)) v
             from decimal_aggregate_state_input) t""")
        def arg256 = fn.endsWith("_map") ? "map(cast(1 as int),cast(v as decimal(76,6)))"
                : "cast(v as decimal(76,6))"
        "order_qt_${fn}_input256"("""select ${fn}(${arg256}),${fn}_merge(${fn}_state(${arg256}))
            from decimal_aggregate_state_input""")
    }
}
