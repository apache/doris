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
    // Constant folding queries
    qt_sql_const_0 "SELECT human_readable_seconds(0);"
    qt_sql_const_1 "SELECT human_readable_seconds(1);"
    qt_sql_const_5 "SELECT human_readable_seconds(5);"
    qt_sql_const_60 "SELECT human_readable_seconds(60);"
    qt_sql_const_3661 "SELECT human_readable_seconds(3661);"
    qt_sql_const_86400 "SELECT human_readable_seconds(86400);"
    qt_sql_const_90061 "SELECT human_readable_seconds(90061);"
    qt_sql_const_999999 "SELECT human_readable_seconds(999999);"
    qt_sql_const_null "SELECT human_readable_seconds(NULL);"
    qt_sql_const_max "SELECT human_readable_seconds(9223372036854775807);"
    qt_sql_const_min "SELECT human_readable_seconds(-9223372036854775808);"
    qt_sql_const_neg "SELECT human_readable_seconds(-100);"

    // Batch column queries (drop before using, preserve after)
    sql "DROP TABLE IF EXISTS test_human_readable_seconds_tbl;"
    sql """
        CREATE TABLE test_human_readable_seconds_tbl (
            id INT,
            val_bigint BIGINT,
            val_int INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_human_readable_seconds_tbl VALUES
            (1, 0, 0),
            (2, 1, 1),
            (3, 5, 5),
            (4, 60, 60),
            (5, 3661, 3661),
            (6, 86400, 86400),
            (7, 90061, 90061),
            (8, 999999, 999999),
            (9, NULL, NULL),
            (10, 9223372036854775807, 2147483647),
            (11, -9223372036854775808, -2147483648),
            (12, -1, -1);
    """

    order_qt_sql_batch_bigint "SELECT id, human_readable_seconds(val_bigint) FROM test_human_readable_seconds_tbl ORDER BY id;"
    order_qt_sql_batch_int "SELECT id, human_readable_seconds(val_int) FROM test_human_readable_seconds_tbl ORDER BY id;"
}
