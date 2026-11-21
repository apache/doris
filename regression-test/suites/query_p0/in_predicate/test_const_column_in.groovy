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

suite("test_const_column_in") {
    sql "DROP TABLE IF EXISTS test_const_column_in"
    sql """
        CREATE TABLE test_const_column_in (
            id INT,
            v BIGINT NULL,
            s STRING NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_const_column_in VALUES
            (1, 1, 's1'),
            (2, 8191, 's8191'),
            (3, 8192, 'missing'),
            (4, NULL, NULL),
            (5, 0, '')
    """

    qt_small_list """
        SELECT id, v IN (1), v NOT IN (1), v IN (1, 1, NULL),
               s IN ('', 's1'), s NOT IN ('', 's1', NULL)
        FROM test_const_column_in ORDER BY id
    """

    def integers = (0..<4096).collect { 2 * it + 1 }.join(', ')
    qt_large_integer_list """
        SELECT id, v IN (${integers}), v NOT IN (${integers})
        FROM test_const_column_in ORDER BY id
    """

    def strings = (0..<4096).collect { "'s${2 * it + 1}'" }.join(', ')
    def longString = 'x' * 8192
    qt_large_string_list """
        SELECT id, s IN (${strings}, '${longString}', ''),
               s NOT IN (${strings}, '${longString}', '')
        FROM test_const_column_in ORDER BY id
    """
}
