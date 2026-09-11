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

suite("test_agg_state_cast") {
    sql "set enable_agg_state = true"
    sql "drop table if exists test_agg_state_cast"
    sql """
        create table test_agg_state_cast (
            id int,
            s agg_state<sum_map(map<string,int>)> generic
        ) aggregate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """

    for (def strict : [false, true]) {
        sql "set enable_strict_cast = ${strict}"
        // Both well-formed and malformed bytes must be rejected without inspecting their payload.
        for (def input : [
            "unhex('000101010161010100000000000000')",
            "unhex('00020101016101010000000000000001010161010300000000000000')",
            "cast('invalid state' as variant)",
            "cast('invalid state' as varbinary)",
            "1"
        ]) {
            test {
                sql "select cast(${input} as agg_state<sum_map(map<string,int>)>)"
                exception "cast"
            }
            test {
                sql "insert into test_agg_state_cast values (1, ${input})"
                exception "cast"
            }
        }
    }
}
