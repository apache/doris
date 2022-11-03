
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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("txn_insert") {
    def table = "txn_insert_tbl"
    sql """ DROP TABLE IF EXISTS $table """
    sql """
        create table $table (
            k1 int, 
            k2 double,
            k3 varchar(100),
            k4 array<int>,
            k5 array<boolean>
        ) distributed by hash(k1) buckets 1
        properties("replication_num" = "1"); 
    """

    // begin and commit
    sql """begin"""
    sql """insert into $table values(1, 2.2, "abc", [], [])"""
    sql """insert into $table values(2, 3.3, "xyz", [1], [1, 0])"""
    sql """insert into $table values(null, null, null, [null], [null, 0])"""
    sql "commit"
    sql "sync"
    order_qt_select1 """select * from $table"""

    // begin and rollback
    sql "begin"
    sql """insert into $table values(1, 2.2, "abc", [], [])"""
    sql """insert into $table values(2, 3.3, "xyz", [1], [1, 0])"""
    sql "rollback"
    sql "sync"
    order_qt_select2 """select * from $table"""

    // begin 2 times and commit
    sql "begin"
    sql """insert into $table values(1, 2.2, "abc", [], [])"""
    sql """insert into $table values(2, 3.3, "xyz", [1], [1, 0])"""
    sql "begin"
    sql """insert into $table values(1, 2.2, "abc", [], [])"""
    sql """insert into $table values(2, 3.3, "xyz", [1], [1, 0])"""
    sql "commit"
    sql "sync"
    order_qt_select3 """select * from $table"""

    // begin 2 times and rollback
    sql "begin"
    sql """insert into $table values(1, 2.2, "abc", [], [])"""
    sql """insert into $table values(2, 3.3, "xyz", [1], [1, 0])"""
    sql "begin"
    sql """insert into $table values(1, 2.2, "abc", [], [])"""
    sql """insert into $table values(2, 3.3, "xyz", [1], [1, 0])"""
    sql "rollback"
    sql "sync"
    order_qt_select4 """select * from $table"""
}
