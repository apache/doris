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

// Test set operations (EXCEPT/INTERSECT) with large string data that exceeds 4GB total.
// This exercises the convert_column_if_overflow path in SetSinkOperatorX::_process_build_block.
suite("test_set_operation_large_string") {
    def totalRows = 4210
    sql """ DROP TABLE IF EXISTS test_set_op_large_string """
    sql """
        CREATE TABLE test_set_op_large_string (
            id INT NULL,
            large_val STRING NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // Each row has a ~1MB string. Insert 4200 rows to exceed 4GB total string data.
    // Use repeat() to generate large strings efficiently.
    // Insert in batches to avoid timeout.
    def batchSize = 500
    def strSizePerRow = 1048576 // 1MB

    // repeat('x', 1048576) produces a 1MB string per row.
    // We concat the id to make each row unique.
    for (int batchStart = 0; batchStart < totalRows; batchStart += batchSize) {
        def batchEnd = Math.min(batchStart + batchSize, totalRows)
        sql """
            INSERT INTO test_set_op_large_string
            SELECT number, concat(cast(number as string), repeat('x', ${strSizePerRow}))
            FROM numbers("number" = "${batchEnd}")
            WHERE number >= ${batchStart};
        """
    }

    def rowCount = sql """ SELECT count(*) FROM test_set_op_large_string """
    log.info("Inserted rows: ${rowCount}")
    assert rowCount[0][0] == totalRows

    sql " set parallel_pipeline_task_num = 1;"
    sql " set enable_profile = true;"
    sql " set batch_size = 128;"
    sql " set runtime_filter_mode = off;"
    sql """ set disable_nereids_rules = "INFER_SET_OPERATOR_DISTINCT";"""
    // Test EXCEPT: all rows from test_set_op_large_string except a small subset should return most rows
    qt_except_subset """
        SELECT substring(large_val, 1, 100) FROM (
            SELECT large_val FROM test_set_op_large_string
            EXCEPT
            SELECT large_val FROM test_set_op_large_string WHERE id < 4208
        ) t
        order by 1;
    """

    // Test EXCEPT: table minus itself should be empty
    qt_except_self """
        SELECT * FROM (
            SELECT large_val FROM test_set_op_large_string
            EXCEPT
            SELECT large_val FROM test_set_op_large_string
        ) t
        order by 1;
    """

    // Test INTERSECT: intersection with a small subset should return the subset
    qt_intersect_subset """
        SELECT substring(large_val, 1, 100) FROM (
            SELECT large_val FROM test_set_op_large_string
            INTERSECT
            SELECT large_val FROM test_set_op_large_string WHERE id < 2
        ) t
        order by 1;
    """

    // Test INTERSECT: table with itself should return all rows
    qt_intersect_self """
        SELECT count(*) FROM (
            SELECT large_val FROM test_set_op_large_string
            INTERSECT
            SELECT large_val FROM test_set_op_large_string
        ) t
        order by 1;
    """

    sql """ DROP TABLE IF EXISTS test_set_op_large_string """
}
