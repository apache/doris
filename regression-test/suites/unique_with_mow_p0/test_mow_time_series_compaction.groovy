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

suite("test_mow_time_series_compaction") {
    def tableName = "test_mow_time_series_compaction"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    test {
        sql """ CREATE TABLE ${tableName}
                (k int, v1 int, v2 int )
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH (k) 
                BUCKETS 1  PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write"="true",
                    "compaction_policy" = "time_series");
            """
        exception "Time series compaction policy is not supported for unique key table"
    }

    tableName = "test_mor_time_series_compaction"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    test {
        sql """ CREATE TABLE ${tableName}
                (k int, v1 int, v2 int )
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH (k) 
                BUCKETS 1  PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write"="false",
                    "compaction_policy" = "time_series");
            """
        exception "Time series compaction policy is not supported for unique key table"
    }

    tableName = "test_mow_time_series_compaction_2"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName}
            (k int, v1 int, v2 int )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH (k) 
            BUCKETS 1  PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write"="true");
        """
    sql "insert into ${tableName} values (1, 1, 1),(2,2,2),(3,3,3);"
    test {
        sql "alter table ${tableName} set (\"compaction_policy\" = \"time_series\");"
        exception "Time series compaction policy is not supported for unique key table"
    }

    tableName = "test_mor_time_series_compaction_2"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName}
            (k int, v1 int, v2 int )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH (k) 
            BUCKETS 1  PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write"="false");
        """
    sql "insert into ${tableName} values (1, 1, 1),(2,2,2),(3,3,3);"
    test {
        sql "alter table ${tableName} set (\"compaction_policy\" = \"time_series\");"
        exception "Time series compaction policy is not supported for unique key table"
    }

}