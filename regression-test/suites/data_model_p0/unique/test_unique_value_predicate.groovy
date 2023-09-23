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

suite("test_unique_value_predicate") {
    // test uniq table
    def tbName = "test_uniq_value_predicate"
    // mor without seq
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                int_value int,
                char_value char(10),
                date_value date
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1",
                       "disable_auto_compaction" = "true",
                       "enable_unique_key_merge_on_write" = "false");
        """
    sql "insert into ${tbName} values(0, 2, 'test int', '2000-02-02')"
    sql "insert into ${tbName} values(0, 1, 'test char', '2000-01-01')"
    qt_select "select * from ${tbName}"
    qt_select "select * from ${tbName} where k = 0"
    qt_select "select * from ${tbName} where k = 0 && int_value = 2"
    qt_select "select * from ${tbName} where k = 0 && int_value = 1"

    // mor with seq
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                int_value int,
                char_value char(10),
                date_value date
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1",
                       "function_column.sequence_col" = "int_value",
                       "disable_auto_compaction" = "true",
                       "enable_unique_key_merge_on_write" = "false");
        """
    sql "insert into ${tbName} values(0, 2, 'test int', '2000-02-02')"
    sql "insert into ${tbName} values(0, 1, 'test char', '2000-01-01')"
    qt_select "select * from ${tbName}"
    qt_select "select * from ${tbName} where k = 0"
    qt_select "select * from ${tbName} where k = 0 && int_value = 2"
    qt_select "select * from ${tbName} where k = 0 && int_value = 1"

    // mow without seq
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                int_value int,
                char_value char(10),
                date_value date
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1",
                       "disable_auto_compaction" = "true",
                       "enable_unique_key_merge_on_write" = "true");
        """
    sql "insert into ${tbName} values(0, 2, 'test int', '2000-02-02')"
    sql "insert into ${tbName} values(0, 1, 'test char', '2000-01-01')"
    qt_select "select * from ${tbName}"
    qt_select "select * from ${tbName} where k = 0"
    qt_select "select * from ${tbName} where k = 0 && int_value = 2"
    qt_select "select * from ${tbName} where k = 0 && int_value = 1"

    // mow with seq
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k int,
                int_value int,
                char_value char(10),
                date_value date
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1",
                       "function_column.sequence_col" = "int_value",
                       "disable_auto_compaction" = "true",
                       "enable_unique_key_merge_on_write" = "true");
        """
    sql "insert into ${tbName} values(0, 2, 'test int', '2000-02-02')"
    sql "insert into ${tbName} values(0, 1, 'test char', '2000-01-01')"
    qt_select "select * from ${tbName}"
    qt_select "select * from ${tbName} where k = 0"
    qt_select "select * from ${tbName} where k = 0 && int_value = 2"
    qt_select "select * from ${tbName} where k = 0 && int_value = 1"
    sql "DROP TABLE ${tbName}"
}
