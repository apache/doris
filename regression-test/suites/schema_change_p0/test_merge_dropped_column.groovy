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

suite("test_merge_dropped_column") {
    def tableName = "test_merge_dropped_column"
    sql """ drop table if exists ${tableName} """
    sql """ create table ${tableName}
            (
                col1 SMALLINT NOT NULL,
                col2 BIGINT NOT NULL,
                col3 TEXT NOT NULL,
                col4 FLOAT NOT NULL,
                col5 TINYINT NOT NULL
            ) DUPLICATE KEY(`col1`) DISTRIBUTED BY HASH (`col1`) BUCKETS 1
            PROPERTIES (
                    "disable_auto_compaction"="true", 
                    "replication_num" = "1"
                );
    """
    sql """ INSERT INTO ${tableName} VALUES(5, 6, "wwwe", 1.2, 3) """
    sql """ DELETE FROM ${tableName} WHERE col1 = 0 """

    sql """ ALTER TABLE ${tableName} DROP COLUMN col3 """
    sql """ INSERT INTO ${tableName} VALUES(7, 1212, 2.22, 2) """
    // sql """ ALTER TABLE ${tableName} ADD COLUMN col3 TEXT NOT DEFAULT NULL """

    sql """ ALTER TABLE ${tableName} MODIFY COLUMN col2 TEXT NOT NULL"""

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }
}