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

suite("test_create_or_replace_view") {
    // create two test tables and insert some data
    sql """DROP TABLE IF EXISTS test_create_or_replace_view_tbl1"""
    sql """
        CREATE TABLE IF NOT EXISTS test_create_or_replace_view_tbl1
        (k1 int, k2 int, v int)
        DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES( "replication_num" = "1");
    """
    sql """DROP TABLE IF EXISTS test_create_or_replace_view_tbl2"""
    sql """
        CREATE TABLE IF NOT EXISTS test_create_or_replace_view_tbl2
        (k1 int, k2 int, v int)
        DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES( "replication_num" = "1");
    """
    sql """INSERT INTO test_create_or_replace_view_tbl1 VALUES(1,1,1)"""
    sql """INSERT INTO test_create_or_replace_view_tbl2 VALUES(2,2,2)"""
    sql "sync"

    // create view
    sql "drop view if exists view_test_create_or_replace_view"
    sql """
        CREATE VIEW IF NOT EXISTS view_test_create_or_replace_view
        AS SELECT * FROM test_create_or_replace_view_tbl1;
    """
    qt_sql_1 """select * from view_test_create_or_replace_view"""

    sql """
        CREATE OR REPLACE VIEW view_test_create_or_replace_view
        AS SELECT * FROM test_create_or_replace_view_tbl2;
    """
    qt_sql_2 """select * from view_test_create_or_replace_view"""
    test {
        sql """
            CREATE OR REPLACE VIEW IF NOT EXISTS view_test_create_or_replace_view
            AS SELECT * FROM test_create_or_replace_view_tbl1;
        """
        exception "[OR REPLACE] and [IF NOT EXISTS] cannot used at the same time"
    }

    sql """drop view if exists view_test_create_or_replace_view"""
    sql """DROP TABLE IF EXISTS test_create_or_replace_view_tbl1"""
    sql """DROP TABLE IF EXISTS test_create_or_replace_view_tbl2"""
}
