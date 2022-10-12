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

suite("test_update_unique", "p0") {
    def tbName = "test_update_unique"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                value1 int,
                value2 int,
                date_value date
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName} values(1, 1, 1, '2000-01-01');"
    sql "insert into ${tbName} values(2, 1, 1, '2000-01-01');"
    sql "UPDATE ${tbName} SET value1 = 2 WHERE k=1;"
    sql "UPDATE ${tbName} SET value1 = value1+1 WHERE k=2;"
    sql "UPDATE ${tbName} SET date_value = '1999-01-01' WHERE k in (1,2);"
    qt_select_uniq_table "select * from ${tbName} order by k"
    sql "UPDATE ${tbName} SET date_value = '1998-01-01' WHERE k is null or k is not null;"
    qt_select_uniq_table "select * from ${tbName} order by k"
    qt_desc_uniq_table "desc ${tbName}"
    sql "DROP TABLE ${tbName}"
}
