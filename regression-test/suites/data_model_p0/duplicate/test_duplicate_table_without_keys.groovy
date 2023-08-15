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

suite("test_duplicate_table_without_keys") {
    def dbName = "test_duplicate_table_without_keys"
    List<List<Object>> db = sql "show databases like '${dbName}'"
    if (db.size() == 0) {
        sql "CREATE DATABASE  ${dbName}"
    }
    sql "use ${dbName}"

    def tbName1 = "test_default_data_model"
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 int,
                k2 int, 
                k3 int,
                int_value int
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 5 
            properties("replication_num" = "1","enable_duplicate_without_keys_by_default" = "false");
        """
    sql "insert into ${tbName1} values(0, 1, 2, 4)"
    sql "insert into ${tbName1} values(0, 1, 2, 5)"
    sql "insert into ${tbName1} values(0, 1, 2, 3)"
    order_qt_select_dup_table "select * from ${tbName1}"
    qt_desc_dup_table "desc ${tbName1}"
    def res = sql "show create table ${tbName1}"
    assertTrue(res.size() != 0)
    sql "DROP TABLE ${tbName1}"
    
    def tbName2 = "test_default_data_model_no_keys"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
                k1 int,
                k2 int, 
                k3 int,
                int_value int
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 5 
            properties("replication_num" = "1","enable_duplicate_without_keys_by_default" = "true");
        """
    sql "insert into ${tbName2} values(0, 1, 2, 4)"
    sql "insert into ${tbName2} values(0, 1, 2, 5)"
    sql "insert into ${tbName2} values(0, 1, 2, 3)"
    order_qt_select_dup_table "select * from ${tbName2}"
    qt_desc_dup_table "desc ${tbName2}"
    res = sql "show create table ${tbName2}"
    assertTrue(res.size() != 0)

    sql "ALTER TABLE ${tbName2} ADD COLUMN new_col1 INT DEFAULT \"0\" AFTER k3"
    order_qt_select_dup_table "select * from ${tbName2}"
    qt_desc_dup_table "desc ${tbName2}"
    res = sql "show create table ${tbName2}"
    assertTrue(res.size() != 0)

    def tbName3 = "test_default_data_model_no_keys_like"
    sql "DROP TABLE IF EXISTS ${tbName3}"
    sql """create table ${tbName3} like ${tbName2}"""
    qt_desc_dup_table "desc ${tbName3}"
    res = sql "show create table ${tbName3}"    
    assertTrue(res.size() != 0)

    sql "DROP TABLE ${tbName2}"
    sql "DROP TABLE ${tbName3}"
}
