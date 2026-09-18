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

// SHOW CREATE TABLE must print predefined field comments as SQL literals that can be replayed.
suite("test_variant_predefine_comment_escape") {
    sql "DROP TABLE IF EXISTS test_variant_predefine_comment_escape"
    sql "DROP TABLE IF EXISTS test_variant_predefine_comment_escape_copy"
    sql """
        CREATE TABLE test_variant_predefine_comment_escape (
            id INT,
            v VARIANT<
                'price':INT COMMENT "O'Reilly",
                'name':STRING COMMENT 'say ''hi'' and "bye"',
                'path':STRING COMMENT 'C:\\\\tmp',
                PROPERTIES ("variant_max_subcolumns_count" = "10")
            >
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    qt_show_create "SHOW CREATE TABLE test_variant_predefine_comment_escape"

    // Replay the printed DDL under a new name; the copy must print the same comments again.
    def ddl = sql("SHOW CREATE TABLE test_variant_predefine_comment_escape")[0][1]
    sql ddl.replace("`test_variant_predefine_comment_escape`", "`test_variant_predefine_comment_escape_copy`")
    qt_show_create_copy "SHOW CREATE TABLE test_variant_predefine_comment_escape_copy"

    sql """INSERT INTO test_variant_predefine_comment_escape_copy
            SELECT 1, parse_to_variant('{"price": 10, "name": "n", "path": "p"}')"""
    order_qt_select_copy "SELECT id, v['price'], v['name'], v['path'] FROM test_variant_predefine_comment_escape_copy"
}
