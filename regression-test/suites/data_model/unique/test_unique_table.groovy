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

suite("test_unique_table", "data_model") {
    def dbName = "test_unique_db"
    List<List<Object>> db = sql "show databases like '${dbName}'"
    if (db.size() == 0) {
        sql "CREATE DATABASE  ${dbName}"
    }
    sql "use ${dbName}"

    // test duplicate table
    def tbName = "test_uniq"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                int_value int,
                char_value char(10),
                date_value date
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName} values(0, 1, 'test char', '2000-01-01')"
    sql "insert into ${tbName} values(0, 2, 'test int', '2000-02-02')"
    sql "insert into ${tbName} values(0, null, null, null)"
    order_qt_select_uniq_table "select * from ${tbName}"
    qt_desc_uniq_table "desc ${tbName}"
    sql "DROP TABLE ${tbName}"
}
