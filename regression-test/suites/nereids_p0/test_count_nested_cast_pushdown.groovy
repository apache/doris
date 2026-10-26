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

suite("test_count_nested_cast_pushdown", "p0") {
    def originalSettings = ["enable_strict_cast", "enable_push_down_no_group_agg"].collectEntries { name ->
        [(name): sql("show variables like '${name}'")[0][1]]
    }
    try {
        sql "drop table if exists test_count_nested_cast_pushdown"
        sql """
            create table test_count_nested_cast_pushdown (
                id int,
                a array<string> not null
            ) duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num"="1")
        """
        sql "insert into test_count_nested_cast_pushdown values (1, ['bad']), (2, ['1'])"
        sql "set enable_strict_cast=true"
        sql "set enable_push_down_no_group_agg=true"

        def queries = [
            "select count(cast(a as array<int>)) from test_count_nested_cast_pushdown",
            "select count(cast_value) from " +
                    "(select cast(a as array<int>) as cast_value from test_count_nested_cast_pushdown) projected"
        ]
        queries.each { query ->
            explain {
                sql(query)
                contains "pushAggOp=NONE"
            }
            test {
                sql(query)
                exception "parse number fail"
            }
        }

        sql "set enable_strict_cast=false"
        def nonStrictQueries = [
            "select assert_true(count(cast(a as array<int>)) = 2, 'wrong direct count') " +
                    "from test_count_nested_cast_pushdown",
            "select assert_true(count(cast_value) = 2, 'wrong projected count') from " +
                    "(select cast(a as array<int>) as cast_value from test_count_nested_cast_pushdown) projected"
        ]
        nonStrictQueries.each { query ->
            explain {
                sql(query)
                contains "pushAggOp=COUNT"
            }
            sql(query)
        }
    } finally {
        originalSettings.each { name, value -> sql "set ${name}=${value}" }
    }
}
