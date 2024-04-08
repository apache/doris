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
                    id int default '10', 
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

        // 7. update stmt
        if (use_nereids_planner) {
            def ut_table = "txn_insert_ut"
            for (def i in 1..2) {
                def tableName = ut_table + "_" + i
                sql """ DROP TABLE IF EXISTS ${tableName} """
                sql """
                    CREATE TABLE ${tableName} (
                        `ID` int(11) NOT NULL,
                        `NAME` varchar(100) NULL,
                        `score` int(11) NULL
                    ) ENGINE=OLAP
                    unique KEY(`id`)
                    COMMENT 'OLAP'
                    DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES (
                        "replication_num" = "1"
                    );
                """
            }
            sql """ insert into ${ut_table}_1 values(1, "a", 100); """
            sql """ begin; """
            sql """ insert into ${ut_table}_2 select * from ${ut_table}_1; """
            sql """ update ${ut_table}_1 set score = 101 where id = 1; """
            sql """ commit; """
            sql "sync"
            order_qt_select25 """select * from ${ut_table}_1 """
            order_qt_select26 """select * from ${ut_table}_2 """
        }

        // 8. delete from using and delete from stmt
        if (use_nereids_planner) {
            for (def ta in ["txn_insert_dt1", "txn_insert_dt2", "txn_insert_dt3", "txn_insert_dt4", "txn_insert_dt5"]) {
                sql """ drop table if exists ${ta} """
            }

            for (def ta in ["txn_insert_dt1", "txn_insert_dt4", "txn_insert_dt5"]) {
                sql """
                    create table ${ta} (
                        id int,
                        dt date,
                        c1 bigint,
                        c2 string,
                        c3 double
                    ) unique key (id, dt)
                    partition by range(dt) (
                        from ("2000-01-01") TO ("2000-01-31") INTERVAL 1 DAY
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1',
                        "enable_unique_key_merge_on_write" = "true"
                    );
                """
                sql """
                    INSERT INTO ${ta} VALUES
                        (1, '2000-01-01', 1, '1', 1.0),
                        (2, '2000-01-02', 2, '2', 2.0),
                        (3, '2000-01-03', 3, '3', 3.0);
                """
            }

            sql """
                create table txn_insert_dt2 (
                    id int,
                    dt date,
                    c1 bigint,
                    c2 string,
                    c3 double
                ) unique key (id)
                distributed by hash(id)
                properties(
                    'replication_num'='1'
                );
            """
            sql """
                create table txn_insert_dt3 (
                    id int
                ) distributed by hash(id)
                properties(
                    'replication_num'='1'
                );
            """
            sql """
                INSERT INTO txn_insert_dt2 VALUES
                    (1, '2000-01-10', 10, '10', 10.0),
                    (2, '2000-01-20', 20, '20', 20.0),
                    (3, '2000-01-30', 30, '30', 30.0),
                    (4, '2000-01-04', 4, '4', 4.0),
                    (5, '2000-01-05', 5, '5', 5.0);
            """
            sql """
                INSERT INTO txn_insert_dt3 VALUES(1),(2),(4),(5);
            """
            sql """ begin """
            test {
                sql '''
                    delete from txn_insert_dt1 temporary partition (p_20000102)
                    using txn_insert_dt2 join txn_insert_dt3 on txn_insert_dt2.id = txn_insert_dt3.id
                    where txn_insert_dt1.id = txn_insert_dt2.id;
                '''
                exception 'Partition: p_20000102 is not exists'
            }
            sql """
                delete from txn_insert_dt1 partition (p_20000102)
                using txn_insert_dt2 join txn_insert_dt3 on txn_insert_dt2.id = txn_insert_dt3.id
                where txn_insert_dt1.id = txn_insert_dt2.id;
            """
            sql """
                delete from txn_insert_dt4
                using txn_insert_dt2 join txn_insert_dt3 on txn_insert_dt2.id = txn_insert_dt3.id
                where txn_insert_dt4.id = txn_insert_dt2.id;
            """
            sql """
                delete from txn_insert_dt2 where id = 1 or id = 5;
            """
            sql """
                delete from txn_insert_dt5 partition(p_20000102) where id = 1 or id = 5;
            """
            sql """ commit """
            sql """ insert into txn_insert_dt2 VALUES (6, '2000-01-10', 10, '10', 10.0) """
            sql """ insert into txn_insert_dt5 VALUES (6, '2000-01-10', 10, '10', 10.0) """
            sql "sync"
            order_qt_select27 """select * from txn_insert_dt1 """
            order_qt_select28 """select * from txn_insert_dt2 """
            order_qt_select29 """select * from txn_insert_dt4 """
            order_qt_select30 """select * from txn_insert_dt5 """
        }
    }
}
