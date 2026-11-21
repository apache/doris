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

suite("test_agg_state_schema_change") {
    sql "set enable_agg_state=true"
    for (boolean enabled : [true, false]) {
        for (String fn : ["sum", "avg", "sum_map", "avg_map"]) {
            sql "set enable_decimal256=${enabled}"
            sql "drop table if exists agg_state_schema_change"
            def inputType = fn.endsWith("_map") ? "map<int,decimal(18,2)>" : "decimal(18,2)"
            def input = fn.endsWith("_map") ? "map(1,1.23,2,4.56)" : "1.23"
            sql """create table agg_state_schema_change (
                k int, s agg_state<${fn}(${inputType})> generic
            ) aggregate key(k) distributed by hash(k) buckets 1
            properties("replication_num"="1")"""
            sql """insert into agg_state_schema_change
                select 1, ${fn}_state(cast(${input} as ${inputType}))"""
            def query = fn.endsWith("_map")
                    ? "select m[1],m[2],map_size(m) from (select ${fn}_merge(s) m from agg_state_schema_change) t"
                    : "select ${fn}_merge(s) from agg_state_schema_change"
            "order_qt_${fn}_${enabled}_before"(query)
            sql "set enable_decimal256=${!enabled}"
            test {
                sql """alter table agg_state_schema_change
                    modify column s agg_state<${fn}(${inputType})> generic"""
                exception "incompatible cast or aggregate state signature"
            }
            "order_qt_${fn}_${enabled}_after"(query)
            // Rejection must leave the table writable, with its original stored signature.
            sql "set enable_decimal256=${enabled}"
            sql """insert into agg_state_schema_change
                select 1, ${fn}_state(cast(${input} as ${inputType}))"""
            "order_qt_${fn}_${enabled}_after_insert"(query)
        }
    }
}
