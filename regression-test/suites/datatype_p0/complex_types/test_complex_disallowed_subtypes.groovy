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

// DORIS-25584: BITMAP/HLL/JSONB must be rejected as complex-type elements at any nesting depth.
// Previously depth-3 (and deeper) nesting was silently accepted; now all depths are validated.

suite("test_complex_disallowed_subtypes") {
    // ---- setup ----
    sql "DROP TABLE IF EXISTS t_complex_disallowed_subtypes_1"
    sql "DROP TABLE IF EXISTS t_complex_disallowed_subtypes_2"
    sql "DROP TABLE IF EXISTS t_complex_disallowed_subtypes_3"
    sql "DROP TABLE IF EXISTS t_complex_disallowed_subtypes_4"
    sql "DROP TABLE IF EXISTS t_complex_valid"

    // ---- depth-2: must still be rejected (regression guard) ----

    test {
        sql "CREATE TABLE t_complex_disallowed_subtypes_1 (k INT, v ARRAY<BITMAP>) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"
        exception "unsupported sub-type"
    }

    test {
        sql "CREATE TABLE t_complex_disallowed_subtypes_2 (k INT, v MAP<INT, HLL>) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"
        exception "unsupported sub-type"
    }

    // ---- depth-3: were silently accepted before the fix, must now be rejected ----

    test {
        // ARRAY<MAP<BITMAP, INT>>
        sql "CREATE TABLE t_complex_disallowed_subtypes_1 (k INT, v ARRAY<MAP<BITMAP,INT>>) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"
        exception "unsupported sub-type"
    }

    test {
        // ARRAY<MAP<STRING, BITMAP>>
        sql "CREATE TABLE t_complex_disallowed_subtypes_2 (k INT, v ARRAY<MAP<STRING,BITMAP>>) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"
        exception "unsupported sub-type"
    }

    test {
        // STRUCT<a: ARRAY<HLL>>
        sql "CREATE TABLE t_complex_disallowed_subtypes_3 (k INT, v STRUCT<a:ARRAY<HLL>>) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"
        exception "unsupported sub-type"
    }

    test {
        // MAP<STRING, ARRAY<JSONB>>
        sql "CREATE TABLE t_complex_disallowed_subtypes_4 (k INT, v MAP<STRING,ARRAY<JSON>>) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"
        exception "unsupported sub-type"
    }

    // ---- valid deep nesting must still be accepted ----

    sql """
        CREATE TABLE t_complex_valid (k INT, v ARRAY<MAP<STRING, INT>>)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    sql "DROP TABLE IF EXISTS t_complex_valid"
}
