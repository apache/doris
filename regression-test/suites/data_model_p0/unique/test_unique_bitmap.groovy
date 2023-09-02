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

suite("test_unique_table_bitmap") {
    def tbName = "test_uniq_table_bitmap1"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                id_bitmap bitmap
            ) UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1");
        """
    sql "insert into ${tbName} values(1,to_bitmap(1));"
    sql "insert into ${tbName} values(2,bitmap_or(to_bitmap(3),to_bitmap(1000)));"
    sql "insert into ${tbName} values(3,bitmap_or(to_bitmap(999),to_bitmap(1000),to_bitmap(888888)));"
    qt_sql "select k,bitmap_count(id_bitmap),bitmap_to_string(id_bitmap) from ${tbName} order by k;"
    sql "insert into ${tbName} values(3,bitmap_from_string('1,0,1,2,3,1,5,99,876,2445'));"
    sql "insert into ${tbName} values(1,bitmap_or(bitmap_from_string('90,5,876'),to_bitmap(1000)));"
    qt_sql "select k,bitmap_count(id_bitmap),bitmap_to_string(id_bitmap) from ${tbName} order by k;"
    sql "DROP TABLE ${tbName};"

    def tbName2 = "test_uniq_table_bitmap2"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
                k int,
                id_bitmap bitmap
            ) UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 
            properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """
    sql "insert into ${tbName2} values(1,to_bitmap(1));"
    sql "insert into ${tbName2} values(2,bitmap_or(to_bitmap(3),to_bitmap(1000)));"
    sql "insert into ${tbName2} values(3,bitmap_or(to_bitmap(999),to_bitmap(1000),to_bitmap(888888)));"
    qt_sql "select k,bitmap_count(id_bitmap),bitmap_to_string(id_bitmap) from ${tbName2} order by k;"
    sql "insert into ${tbName2} values(3,bitmap_from_string('1,0,1,2,3,1,5,99,876,2445'));"
    sql "insert into ${tbName2} values(1,bitmap_or(bitmap_from_string('90,5,876'),to_bitmap(1000)));"
    qt_sql "select k,bitmap_count(id_bitmap),bitmap_to_string(id_bitmap) from ${tbName2} order by k;"
    sql "DROP TABLE ${tbName2};"
}
