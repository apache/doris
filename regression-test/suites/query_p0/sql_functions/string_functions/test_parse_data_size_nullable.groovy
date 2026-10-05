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

suite("test_parse_data_size_nullable") {
    sql "DROP TABLE IF EXISTS test_parse_data_size_nullable"
    sql """
        CREATE TABLE test_parse_data_size_nullable (
            id INT NOT NULL,
            value STRING NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_parse_data_size_nullable VALUES
        (1, NULL), (2, '1MB'), (3, NULL), (4, '2.5MB'), (5, '0B'), (6, NULL)"""

    qt_mixed "SELECT id, parse_data_size(value) FROM test_parse_data_size_nullable ORDER BY id"
    qt_all_null """SELECT id, parse_data_size(value) FROM test_parse_data_size_nullable
        WHERE value IS NULL ORDER BY id"""
    qt_non_null """SELECT id, parse_data_size(value) FROM test_parse_data_size_nullable
        WHERE value IS NOT NULL ORDER BY id"""
    qt_constants "SELECT parse_data_size(NULL), parse_data_size('1MB')"
    qt_empty "SELECT parse_data_size(value) FROM test_parse_data_size_nullable WHERE id < 0"

    sql "INSERT INTO test_parse_data_size_nullable VALUES (7, '')"
    test {
        sql "SELECT parse_data_size(value) FROM test_parse_data_size_nullable ORDER BY id"
        exception 'Invalid Input argument "" of function parse_data_size'
    }
    sql "INSERT INTO test_parse_data_size_nullable VALUES (8, 'invalid')"
    test {
        sql """SELECT parse_data_size(value) FROM test_parse_data_size_nullable
            WHERE id IN (1, 8) ORDER BY id"""
        exception 'Invalid Input argument "invalid" of function parse_data_size'
    }
}
