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

suite("test_human_readable_seconds") {
    // 1. FE-vs-BE equivalence tests (runs both FE folding and BE execution and verifies identical output)
    testFoldConst("SELECT human_readable_seconds(0);")
    testFoldConst("SELECT human_readable_seconds(1);")
    testFoldConst("SELECT human_readable_seconds(60);")
    testFoldConst("SELECT human_readable_seconds(-60);")
    testFoldConst("SELECT human_readable_seconds(61);")
    testFoldConst("SELECT human_readable_seconds(-61);")
    testFoldConst("SELECT human_readable_seconds(604800);")
    testFoldConst("SELECT human_readable_seconds(604801);")
    testFoldConst("SELECT human_readable_seconds(1209600);")
    testFoldConst("SELECT human_readable_seconds(3601);")
    testFoldConst("SELECT human_readable_seconds(3660);")
    testFoldConst("SELECT human_readable_seconds(8003);")
    testFoldConst("SELECT human_readable_seconds(56363463);")
    testFoldConst("SELECT human_readable_seconds(535333.9513888889);")
    testFoldConst("SELECT human_readable_seconds(0.5);")
    testFoldConst("SELECT human_readable_seconds(2.5);")
    testFoldConst("SELECT human_readable_seconds(-2.5);")
    testFoldConst("SELECT human_readable_seconds(9223372036854775807);")
    testFoldConst("SELECT human_readable_seconds(-9223372036854775808);")
    testFoldConst("SELECT human_readable_seconds(9223372036854775295);")
    testFoldConst("SELECT human_readable_seconds(1e19);")
    testFoldConst("SELECT human_readable_seconds(cast('nan' as double));")
    testFoldConst("SELECT human_readable_seconds(cast('inf' as double));")
    testFoldConst("SELECT human_readable_seconds(cast('-inf' as double));")
    testFoldConst("SELECT human_readable_seconds(NULL);")

    // Batch column queries (drop before using, preserve after)
    sql "DROP TABLE IF EXISTS test_human_readable_seconds_tbl;"
    sql """
        CREATE TABLE test_human_readable_seconds_tbl (
            id INT,
            val_double DOUBLE,
            val_bigint BIGINT,
            val_int INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_human_readable_seconds_tbl VALUES
            (1, 0.0, 0, 0),
            (2, 1.0, 1, 1),
            (3, 60.0, 60, 60),
            (4, -60.0, -60, -60),
            (5, 61.0, 61, 61),
            (6, 3601.0, 3601, 3601),
            (7, 3660.0, 3660, 3660),
            (8, 8003.0, 8003, 8003),
            (9, 56363463.0, 56363463, 56363463),
            (10, 535333.9513888889, 535334, 535334),
            (11, NULL, NULL, NULL);
    """

    order_qt_sql_batch_double "SELECT id, human_readable_seconds(val_double) FROM test_human_readable_seconds_tbl ORDER BY id;"
    order_qt_sql_batch_bigint "SELECT id, human_readable_seconds(val_bigint) FROM test_human_readable_seconds_tbl ORDER BY id;"
    order_qt_sql_batch_int "SELECT id, human_readable_seconds(val_int) FROM test_human_readable_seconds_tbl ORDER BY id;"
}
