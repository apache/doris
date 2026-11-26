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

suite("test_lambda_subquery_rejected") {
    // Reject every subquery kind before binding or folding the lambda body.
    def rejectedQueries = [
        "SELECT array_exists(x -> x IN (SELECT 1), [1, 2])",
        "SELECT array_exists(x -> x NOT IN (SELECT 1), [1, 2])",
        "SELECT array_filter(x -> x IN (SELECT 'evil.com'), ['evil.com', 'ok.com'])",
        "SELECT array_exists(x -> EXISTS (SELECT 1 WHERE x = 1), [1, 2])",
        "SELECT array_exists(x -> NOT EXISTS (SELECT 1 WHERE x = 1), [1, 2])",
        "SELECT array_exists(x -> EXISTS (SELECT 1), [1, 2])",
        "SELECT array_exists(x -> EXISTS (SELECT count(*) FROM (SELECT 1) t), [1, 2])",
        "SELECT array_map(x -> concat(x, (SELECT 'suffix')), ['a', 'b'])",
        "SELECT array_map(x -> (SELECT x), [1, 2])",
        "SELECT array_map(x -> (SELECT 1 WHERE t.s > 0), a) FROM (SELECT 1 AS s, [1, 2] AS a) t",
        "SELECT array_map(x -> if(false, (SELECT 1), x), [1, 2])",
        "SELECT array_map(x -> array_map(y -> y + (SELECT 1), [1]), [1])",
        "SELECT array_map(x -> array_map(y -> y, (SELECT [1])), [1])",
        "SELECT array_sort((x, y) -> (SELECT 1), [1, 2])",
        "SELECT map_filter((k, v) -> EXISTS (SELECT 1), map(1, 2))",
        "SELECT 1 WHERE array_exists(x -> x IN (SELECT 1), [1, 2])"
    ]
    rejectedQueries.each { query ->
        test {
            sql query
            exception "Subqueries are not supported in lambda expressions"
        }
    }

    // A subquery outside the body may still supply the array or contain a lambda.
    explain {
        sql "SELECT array_map(x -> x + 1, (SELECT [1, 2]))"
        contains "PLAN FRAGMENT"
    }
    explain {
        sql "SELECT array_filter(x -> x IN (1, 2), (SELECT [1, 2, 3]))"
        contains "PLAN FRAGMENT"
    }
    explain {
        sql "SELECT (SELECT array_map(x -> x + 1, [1, 2]))"
        contains "PLAN FRAGMENT"
    }
}
