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

suite("test_nereids_show_alter_table") {
    sql """DROP DATABASE IF EXISTS test_show_alter_table_db;"""
    sql """CREATE DATABASE IF NOT EXISTS test_show_alter_table_db;"""
    sql """DROP TABLE IF EXISTS test_show_alter_table_db.test_show_alter_table_tbl"""
    sql """DROP TABLE IF EXISTS test_show_alter_table_db.test_show_alter_table_tbl1"""
    sql """DROP TABLE IF EXISTS test_show_alter_table_db.test_show_alter_table_tbl2"""

    sql """
        CREATE  TABLE IF NOT EXISTS test_show_alter_table_db.test_show_alter_table_tbl(id int, name int) ENGINE = olap duplicate key(id) DISTRIBUTED BY HASH(`id`) BUCKETS 2 PROPERTIES ('replication_num' = '1');
    """
    sql """
        CREATE  TABLE IF NOT EXISTS test_show_alter_table_db.test_show_alter_table_tbl1(id int, name int) ENGINE = olap duplicate key(id) DISTRIBUTED BY HASH(`id`) BUCKETS 2 PROPERTIES ('replication_num' = '1');
    """
    sql """
        CREATE  TABLE IF NOT EXISTS test_show_alter_table_db.test_show_alter_table_tbl2(id int, name int) ENGINE = olap duplicate key(id) DISTRIBUTED BY HASH(`id`) BUCKETS 2 PROPERTIES ('replication_num' = '1');
    """
    sql """insert into test_show_alter_table_db.test_show_alter_table_tbl values (1,1),(1,2),(1,3);"""
    sql """create materialized view test_show_alter_table_db.a_mv as select id as a1, sum(name) as a2 from test_show_alter_table_db.test_show_alter_table_tbl group by id;
    """
    sleep(10000)
    sql """create materialized view test_show_alter_table_db.b_mv as select id as a3 from test_show_alter_table_db.test_show_alter_table_tbl group by id;
    """
    sleep(10000)
    sql """ALTER TABLE test_show_alter_table_db.test_show_alter_table_tbl1  MODIFY COLUMN name BIGINT AFTER id;"""
    sql """ALTER TABLE test_show_alter_table_db.test_show_alter_table_tbl2  MODIFY COLUMN name BIGINT AFTER id;"""
    sleep(10000)

    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db;")
    checkNereidsExecute("show alter table materialized view in test_show_alter_table_db;")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db order by JobId;")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db order by JobId desc;")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where TableName = 'table_right';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where IndexName = 'table_right';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where CreateTime = '2025-06-04 20:58:27';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where FinishTime = '2025-06-04 20:58:27';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where State = 'FINISHED';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where State = 'FINISHED' order by JobId;")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where State = 'FINISHED' order by IndexName;")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where State = 'FINISHED' order by IndexName limit 1;")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where State = 'FINISHED' order by IndexName limit 1,1;")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where State = 'FINISHED' and CreateTime = '2025-06-04 21:01:50';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where FinishTime != '2025-06-04 21:53:48';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where FinishTime >= '2025-06-04 21:53:48';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where FinishTime > '2025-06-04 21:53:48';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where FinishTime <= '2025-06-04 21:53:48';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where FinishTime < '2025-06-04 21:53:48';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where TableName != 'table_right';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where CreateTime >= '2025-06-05 22:48:08';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where CreateTime > '2025-06-05 22:48:08';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where CreateTime <= '2025-06-05 22:48:08';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where CreateTime < '2025-06-05 22:48:08';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where BaseIndexName = 'test_show_alter_table_tbl';")
    checkNereidsExecute("show alter table materialized view from test_show_alter_table_db where RollupIndexName = 'a_mv';")

    checkNereidsExecute("show alter table column from test_show_alter_table_db;")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where TableName = 'table_right1';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db order by JobId desc;")
    checkNereidsExecute("show alter table column from test_show_alter_table_db order by JobId;")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where TableName = 'table_right1' and IndexName = 'table_right2';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where CreateTime = '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where FinishTime = '2025-06-04 21:29:03.653';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where TableName = 'table_right1' and CreateTime = '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where State = 'FINISHED';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where State = 'FINISHED' order by TableName desc limit 1;")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where State = 'FINISHED' order by TableName desc limit 1,1;")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where TableName != 'table_right1';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where CreateTime >= '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where CreateTime <= '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where CreateTime > '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where CreateTime < '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where CreateTime < '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where FinishTime <= '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where FinishTime < '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where FinishTime >= '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where FinishTime > '2025-06-04 21:28:14.037';")
    checkNereidsExecute("show alter table column from test_show_alter_table_db where FinishTime != '2025-06-04 21:28:14.037';")

    def res1 = sql """show alter table materialized view from test_show_alter_table_db"""
    assertEquals(2, res1.size())
    def res2 = sql """show alter table materialized view from test_show_alter_table_db order by IndexName"""
    assertEquals(2, res2.size())
    assertEquals("a_mv", res2.get(0).get(5))
    def res3 = sql """show alter table materialized view from test_show_alter_table_db order by IndexName limit 1"""
    assertEquals(1, res3.size())
    assertEquals("a_mv", res3.get(0).get(5))

    def res5 = sql """show alter table column from test_show_alter_table_db;"""
    assertEquals(2, res5.size())
    def res6 = sql """show alter table column from test_show_alter_table_db order by TableName;"""
    assertEquals(2, res6.size())
    assertEquals("test_show_alter_table_tbl1", res6.get(0).get(1))
    def res7 = sql """show alter table column from test_show_alter_table_db where TableName = 'test_show_alter_table_tbl1';"""
    assertEquals(1, res7.size())
    assertEquals("test_show_alter_table_tbl1", res7.get(0).get(1))

    def res8 = sql """show alter table materialized view from test_show_alter_table_db where BaseIndexName = 'test_show_alter_table_tbl';"""
    assertEquals(2, res8.size())
    assertEquals("test_show_alter_table_tbl", res8.get(0).get(1))

    def res9 = sql """show alter table materialized view from test_show_alter_table_db where RollupIndexName = 'a_mv';"""
    assertEquals(1, res9.size())
    assertEquals("a_mv", res9.get(0).get(5))

    // the result of 'show alter table materialized view' does not contain IndexName, so this SQL will return empty set
    def res10 = sql """show alter table materialized view from test_show_alter_table_db where IndexName = 'a_mv';"""
    assertEquals(0, res10.size())

    assertThrows(Exception.class, {
        sql """show alter table materialized view from test_show_alter_table_db where JobId = 1749041691284;"""
    })
}
