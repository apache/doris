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

suite("test_minmax_cast_pushdown", "p0") {
    def originalSettings = ["enable_strict_cast", "enable_push_down_no_group_agg"].collectEntries { name ->
        [(name): sql("show variables like '${name}'")[0][1]]
    }
    try {
        sql "drop table if exists test_minmax_cast_pushdown"
        sql """
            create table test_minmax_cast_pushdown (
                id int,
                value bigint
            ) duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num"="1")
        """
        // One insert into one tablet keeps the valid interior value between overflowing zone-map endpoints.
        sql "insert into test_minmax_cast_pushdown values (1, -2147483649), (2, 0), (3, 2147483648)"
        sql "set enable_strict_cast=false"

        def queries = [
            "select min(cast(value as int)) from test_minmax_cast_pushdown",
            "select max(cast(value as int)) from test_minmax_cast_pushdown",
            "select min(cast(value as int)), max(cast(value as int)) from test_minmax_cast_pushdown",
            "select min(cast_value), max(cast_value) from " +
                    "(select cast(value as int) as cast_value from test_minmax_cast_pushdown) projected"
        ]
        queries.each { query ->
            sql "set enable_push_down_no_group_agg=false"
            def fullScanResult = sql(query)
            sql "set enable_push_down_no_group_agg=true"
            assertEquals(fullScanResult, sql(query))
            explain {
                sql(query)
                contains "pushAggOp=NONE"
            }
        }
        explain {
            sql "select min(value), max(value) from test_minmax_cast_pushdown"
            contains "pushAggOp=MINMAX"
        }
        explain {
            sql "select min(cast(id as bigint)), max(cast(id as bigint)) from test_minmax_cast_pushdown"
            contains "pushAggOp=MINMAX"
        }
    } finally {
        originalSettings.each { name, value -> sql "set ${name}=${value}" }
    }
}
