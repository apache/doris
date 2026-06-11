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

suite("test_scan_filtered_rows_not_pollute_load_counter", "p0") {
    // Rows filtered by scan predicates of a query must not be counted as load
    // "unselected" rows. Otherwise loadedRows reported to FE becomes negative
    // (total 0 - unselected N) and the insert fails with errors like
    // "Insert has too many filtered data 0/-10 insert_max_filter_ratio is 1.000000".
    def srcTable = "test_scan_filter_load_counter_src"
    def dstTable = "test_scan_filter_load_counter_dst"
    def uniqTable = "test_scan_filter_load_counter_uniq"

    sql """ DROP TABLE IF EXISTS ${srcTable} """
    // Predicates on value columns of an AGGREGATE KEY table can neither be pushed
    // down as column predicates nor as common expressions, so they are evaluated
    // by the scanner conjuncts and counted into ScannerCounter.num_rows_unselected.
    sql """
        CREATE TABLE ${srcTable} (
            k1 INT,
            v1 INT REPLACE
        ) AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO ${srcTable} VALUES
            (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)
    """

    sql """ DROP TABLE IF EXISTS ${dstTable} """
    sql """
        CREATE TABLE ${dstTable} (
            k1 INT
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql "set enable_insert_strict=false"
    sql "set insert_max_filter_ratio=1"

    // All 10 scanned rows are filtered inside the scanner; the insert must
    // succeed as a no-op instead of failing the filter ratio check.
    sql """ INSERT INTO ${dstTable} SELECT k1 FROM ${srcTable} WHERE v1 > 1000 """
    qt_insert_empty "select count(*) from ${dstTable}"

    // Same with profile enabled, which was the original trigger of this issue.
    sql "set enable_profile=true"
    sql """ INSERT INTO ${dstTable} SELECT k1 FROM ${srcTable} WHERE v1 > 1000 """
    qt_insert_empty_profile "select count(*) from ${dstTable}"
    sql "set enable_profile=false"

    // DELETE ... WHERE EXISTS executes through the insert path (delete sign).
    // A no-op delete whose source scan filters out all rows must succeed.
    sql """ DROP TABLE IF EXISTS ${uniqTable} """
    sql """
        CREATE TABLE ${uniqTable} (
            k1 INT,
            v1 INT
        ) UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """ INSERT INTO ${uniqTable} VALUES (1,1),(2,2),(3,3) """
    sql """
        DELETE FROM ${uniqTable} t WHERE EXISTS (
            SELECT 1 FROM ${srcTable} s WHERE s.k1 = t.k1 AND s.v1 > 1000
        )
    """
    qt_delete_noop "select count(*) from ${uniqTable}"

    // UPDATE also executes through the insert path; a no-op update whose
    // subquery scan filters out all rows must succeed.
    sql """
        UPDATE ${uniqTable} SET v1 = 100 WHERE k1 IN (
            SELECT k1 FROM ${srcTable} WHERE v1 > 1000
        )
    """
    qt_update_noop "select * from ${uniqTable} order by k1"

    sql """ DROP TABLE IF EXISTS ${srcTable} """
    sql """ DROP TABLE IF EXISTS ${dstTable} """
    sql """ DROP TABLE IF EXISTS ${uniqTable} """
}
