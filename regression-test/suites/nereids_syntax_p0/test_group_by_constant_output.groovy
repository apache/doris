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

suite("test_group_by_constant_output") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // The output alias 'b' collides with the derived-table column 'b', so under only_full_group_by the
    // column 'b' is grouped and the constant column 'a' is left ungrouped. Because 'a' is constant for
    // every input row it is accepted (MySQL functional-dependency behavior), and the query returns (1, 2).
    qt_const_alias_collision "SELECT a as b, b as c FROM (SELECT 1 as a, 2 as b) t1 GROUP BY b, c"

    // A constant column not present in GROUP BY is allowed even though it is neither grouped nor aggregated.
    qt_const_not_in_groupby "SELECT a, b FROM (SELECT 1 as a, 2 as b) t1 GROUP BY a"

    // The same shape over a real table leaves a non-constant column ungrouped, which is still rejected
    // (matching MySQL, where only constant / functionally-dependent columns are allowed).
    sql "DROP TABLE IF EXISTS test_gb_const_t"
    sql """
        CREATE TABLE test_gb_const_t (a INT, b INT) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    test {
        sql "SELECT a as b, b as c FROM test_gb_const_t GROUP BY b, c"
        exception "must appear in the GROUP BY"
    }
}
