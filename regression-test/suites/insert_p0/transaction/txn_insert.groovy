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
    for (def use_nereids_planner : [false, true]) {
        sql " SET enable_nereids_planner = $use_nereids_planner; "
        sql " SET enable_fallback_to_original_planner = false; "

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

        // write to table with mv
        def tableMV = table + "_mv"
        do {
            sql """ DROP TABLE IF EXISTS $tableMV """
            sql """
                create table $tableMV (
                    id int not null, 
                    c1 int default '10'
                ) distributed by hash(id, c1) 
                properties('replication_num'="1");
            """
            createMV """ create materialized view mv_${tableMV} as select c1 from $tableMV; """
            sql "begin"
            sql """insert into $tableMV values(1, 2), (3, 4)"""
            sql """insert into $tableMV values(5, 6)"""
            sql """insert into $tableMV values(7, 8)"""
            sql "commit"
            sql "sync"
            order_qt_select5 """select * from $tableMV"""
            order_qt_select6 """select c1 from $tableMV"""
        } while (0);
        do {
            try {
                sql "begin"
                sql """insert into $tableMV values(9, 2), (10, 4)"""
                sql """insert into $tableMV values(null, 6)"""
                sql "commit"
            } catch (Exception e) {
                sql "rollback"
                logger.info("insert into $tableMV failed: " + e.getMessage())
                assertTrue(e.getMessage().contains("too many filtered rows"))
                assertTrue(e.getMessage().contains("url"))
            }
        } while (0);

        // ------------------- insert into select -------------------
        for (int j = 0; j < 3; j++) {
            def tableName = table + "_" + j
            sql """ DROP TABLE IF EXISTS $tableName """
            sql """
                create table $tableName (
                    k1 int, 
                    k2 double,
                    k3 varchar(100),
                    k4 array<int>,
                    k5 array<boolean>
                ) distributed by hash(k1) buckets 1
                properties("replication_num" = "1"); 
            """
        }

        def result = sql """ show variables like 'enable_fallback_to_original_planner'; """
        logger.info("enable_fallback_to_original_planner: $result")

        // 1. insert into select to 3 tables: batch insert into select only supports nereids planner, and can't fallback
        sql """ begin; """
        if (use_nereids_planner) {
            sql """ insert into ${table}_0 select * from $table; """
            sql """ insert into ${table}_1 select * from $table; """
            sql """ insert into ${table}_2 select * from ${table}_0; """
        } else {
            test {
                sql """ insert into ${table}_0 select * from $table; """
                exception "Insert into ** select is not supported in a transaction"
            }
        }
        sql """ commit; """
        sql "sync"
        order_qt_select7 """select * from ${table}_0"""
        order_qt_select8 """select * from ${table}_1"""
        order_qt_select9 """select * from ${table}_2"""

        // 2. with different label
        if (use_nereids_planner) {
            def label = UUID.randomUUID().toString().replaceAll("-", "")
            def label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql """ begin with label $label; """
            test {
                sql """ insert into ${table}_0 with label $label2 select * from $table; """
                exception "Transaction insert expect label"
            }
            sql """ insert into ${table}_1 select * from $table; """
            sql """ insert into ${table}_2 select * from ${table}_0; """
            sql """ commit; """
            sql "sync"
            order_qt_select10 """select * from ${table}_0"""
            order_qt_select11 """select * from ${table}_1"""
            order_qt_select12 """select * from ${table}_2"""
        }

        // 3. insert into select and values
        if (use_nereids_planner) {
            sql """ begin; """
            sql """ insert into ${table}_0 select * from $table where k1 = 1; """
            test {
                sql """insert into ${table}_1 values(1, 2.2, "abc", [], [])"""
                exception "Transaction insert can not insert into values and insert into select at the same time"
            }
            sql """ insert into ${table}_1 select * from $table where k2 = 2.2 limit 1; """
            sql """ commit; """
            sql "sync"
            order_qt_select13 """select * from ${table}_0"""
            order_qt_select14 """select * from ${table}_1"""
            order_qt_select15 """select * from ${table}_2"""
        }

        // 4. insert into values and select
        if (use_nereids_planner) {
            sql """ begin; """
            sql """insert into ${table}_1 values(100, 2.2, "abc", [], [])"""
            test {
                sql """ insert into ${table}_0 select * from $table; """
                exception "Transaction insert can not insert into values and insert into select at the same time"
            }
            sql """insert into ${table}_1 values(101, 2.2, "abc", [], [])"""
            sql """ commit; """
            sql "sync"
            order_qt_select16 """select * from ${table}_0"""
            order_qt_select17 """select * from ${table}_1"""
            order_qt_select18 """select * from ${table}_2"""
        }

        // 5. rollback
        if (use_nereids_planner) {
            def label = UUID.randomUUID().toString().replaceAll("-", "")
            sql """ begin with label $label; """
            sql """ insert into ${table}_0 select * from $table where k1 = 1; """
            sql """ insert into ${table}_1 select * from $table where k2 = 2.2 limit 1; """
            sql """ rollback; """
            logger.info("rollback $label")
            sql "sync"
            order_qt_select19 """select * from ${table}_0"""
            order_qt_select20 """select * from ${table}_1"""
            order_qt_select21 """select * from ${table}_2"""
        }

        // 6. insert select with error
        if (use_nereids_planner) {
            sql """ begin; """
            test {
                sql """ insert into ${table}_0 select * from $tableMV; """
                exception "insert into cols should be corresponding to the query output"
            }
            sql """ insert into ${table}_1 select * from $table where k2 = 2.2 limit 1; """
            sql """ commit; """
            sql "sync"
            order_qt_select22 """select * from ${table}_0"""
            order_qt_select23 """select * from ${table}_1"""
            order_qt_select24 """select * from ${table}_2"""
        }
    }
}
