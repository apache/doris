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
suite("test_sync_table") {
    def tableName = "test_sync_table_tbl"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `id` INT,
            `name` STRING
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "group_commit_interval_ms" = "10000" -- 10s
        );
    """

    sql """set group_commit = async_mode;"""

    sql """INSERT INTO ${tableName} VALUES (1, "tom"), (2, "jerry");"""

    // cannot view data begin
    qt_select1 """SELECT COUNT(*) FROM ${tableName};"""

    // run sync table
    sql """SYNC TABLE ${tableName};"""

    // can view data
    qt_select2 """SELECT COUNT(*) FROM ${tableName};"""
}
