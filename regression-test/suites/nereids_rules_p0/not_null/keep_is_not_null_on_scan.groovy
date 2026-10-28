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

suite("test_keep_is_not_null_on_scan") {
    def originalThreshold = sql("show variables like 'keep_is_not_null_num_nulls_threshold'")[0][1]
    try {
        sql "drop table if exists test_keep_is_not_null_t1"
        sql "drop table if exists test_keep_is_not_null_t2"
        sql """
            create table test_keep_is_not_null_t1 (
                id int not null,
                val int null
            ) duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        sql """
            create table test_keep_is_not_null_t2 (
                id int not null,
                val int null
            ) duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        sql "insert into test_keep_is_not_null_t1 values (1, 1), (2, null), (3, null)"
        sql "insert into test_keep_is_not_null_t2 values (1, 1), (2, null), (3, null)"
        sql "analyze table test_keep_is_not_null_t1 with sync"
        sql "analyze table test_keep_is_not_null_t2 with sync"

        // `val` has 2 nulls in both tables, so `t1.val is not null` and `t2.val is not null` are inferred
        // from the join condition.
        def joinSql = "select * from test_keep_is_not_null_t1 t1 join test_keep_is_not_null_t2 t2 on t1.val = t2.val"

        // The inferred `is not null` predicates are kept when the column has more nulls than the threshold,
        // so the scan can skip the null values.
        sql "set keep_is_not_null_num_nulls_threshold = 1"
        explain {
            sql joinSql
            contains "IS NOT NULL"
        }

        // The column has only 2 nulls, so the inferred predicates are eliminated when the threshold is larger.
        sql "set keep_is_not_null_num_nulls_threshold = 1000000"
        explain {
            sql joinSql
            notContains "IS NOT NULL"
        }

        // A negative threshold always eliminates the inferred `is not null` predicates.
        sql "set keep_is_not_null_num_nulls_threshold = -1"
        explain {
            sql joinSql
            notContains "IS NOT NULL"
        }
    } finally {
        sql "set keep_is_not_null_num_nulls_threshold = ${originalThreshold}"
    }
}
