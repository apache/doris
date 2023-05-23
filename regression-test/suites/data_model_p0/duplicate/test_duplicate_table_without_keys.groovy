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

suite("test_duplicate_table") {
    def dbName = "test_duplicate_db"
    List<List<Object>> db = sql "show databases like '${dbName}'"
    if (db.size() == 0) {
        sql "CREATE DATABASE  ${dbName}"
    }
    sql "use ${dbName}"
    
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
    qt_show_dup_table "show create table ${tbName2}"

    sql "ALTER TABLE ${tbName2} ADD COLUMN new_col1 INT DEFAULT "0" AFTER k3"
    order_qt_select_dup_table "select * from ${tbName2}"
    qt_desc_dup_table "desc ${tbName2}"
    qt_show_dup_table "show create table ${tbName2}"

    sql "DROP TABLE ${tbName2}"
}
