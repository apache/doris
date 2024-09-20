
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

suite("test_mow_enable_sequence_col") {

    def tableName = "test_mow_enable_sequence_col"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
    sql """CREATE TABLE IF NOT EXISTS ${tableName}
            (`user_id` BIGINT NOT NULL,
            `username` VARCHAR(50) NOT NULL,
            `city` VARCHAR(20),
            `age` SMALLINT)
            UNIQUE KEY(`user_id`)
            DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
            PROPERTIES (
            "disable_auto_compaction" = true,
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true");"""

    sql """insert into ${tableName}(`user_id`,`username`,`city`,`age`) VALUES(111,'aaa','bbb',11);"""
    sql """insert into ${tableName}(`user_id`,`username`,`city`,`age`) VALUES(222,'bbb','bbb',11);"""
    sql """insert into ${tableName}(`user_id`,`username`,`city`,`age`) VALUES(333,'ccc','ddd',11);"""
    order_qt_sql "select * from ${tableName};"

    sql "set show_hidden_columns = true;"
    sql "sync;"
    def res = sql "desc ${tableName} all;"
    assertTrue(!res.toString().contains("__DORIS_SEQUENCE_COL__"))
    sql "set show_hidden_columns = false;"
    sql "sync;"

    def doSchemaChange = { cmd ->
        sql cmd
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
            time 2000
        }
    }
    doSchemaChange """ALTER TABLE ${tableName} ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "bigint");"""
    
    sql "set show_hidden_columns = true;"
    sql "sync;"
    res = sql "desc ${tableName} all;"
    assertTrue(res.toString().contains("__DORIS_SEQUENCE_COL__"))
    order_qt_sql "select * from ${tableName};"
    sql "set show_hidden_columns = false;"
    sql "sync;"

    sql """insert into ${tableName}(`user_id`,`username`,`city`,`age`, `__DORIS_SEQUENCE_COL__`) VALUES(111,'zzz','yyy',100,99);"""
    sql """insert into ${tableName}(`user_id`,`username`,`city`,`age`, `__DORIS_SEQUENCE_COL__`) VALUES(111,'hhh','mmm',200,88);"""
    sql """insert into ${tableName}(`user_id`,`username`,`city`,`age`, `__DORIS_SEQUENCE_COL__`) VALUES(222,'qqq','ppp',300,77);"""
    sql """insert into ${tableName}(`user_id`,`username`,`city`,`age`, `__DORIS_SEQUENCE_COL__`) VALUES(222,'xxx','www',400,99);"""

    sql "set show_hidden_columns = true;"
    sql "sync;"
    order_qt_sql "select * from ${tableName};"
}
