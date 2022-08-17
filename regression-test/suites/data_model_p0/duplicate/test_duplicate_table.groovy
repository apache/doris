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

suite("test_duplicate_table", "data_model") {
    def dbName = "test_duplicate_db"
    List<List<Object>> db = sql "show databases like '${dbName}'"
    if (db.size() == 0) {
        sql "CREATE DATABASE  ${dbName}"
    }
    sql "use ${dbName}"
    
    // test duplicate table
    def tbName = "test_dup"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                int_value int,
                char_value char(10),
                date_value date
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1")
        """
    sql "insert into ${tbName} values(0, 1, 'test char', '2000-01-01')"
    sql "insert into ${tbName} values(0, 2, 'test int', '2000-02-02')"
    sql "insert into ${tbName} values(0, null, null, null)"
    order_qt_select_dup_table "select * from ${tbName}"
    qt_desc_dup_table "desc ${tbName}"
    sql "DROP TABLE ${tbName}"

    // test default data model is duplicate
    def tbName1 = "test_default_data_model"
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 int,
                k2 int, 
                k3 int,
                int_value int
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName1} values(0, 1, 2, 4)"
    sql "insert into ${tbName1} values(0, 1, 2, 5)"
    sql "insert into ${tbName1} values(0, 1, 2, 3)"
    order_qt_select_dup_table "select * from ${tbName1}"
    qt_desc_dup_table "desc ${tbName1}"
    sql "DROP TABLE ${tbName1}"
}
