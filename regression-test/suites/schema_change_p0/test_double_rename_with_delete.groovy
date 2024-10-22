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

suite("test_double_rename_with_delete") {
    def tableName = "test_double_rename_with_delete"
    sql """
        CREATE TABLE ${tableName}
        (
            col1 BIGINT NOT NULL,col2 TINYINT NOT NULL
        )
        DUPLICATE KEY(`col1`,`col2`)
        DISTRIBUTED BY HASH(`col1`,`col2`) BUCKETS 4
        PROPERTIES (
            "disable_auto_compaction"="true",
            "replication_num" = "1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES(2000, 1) """
    sql """ INSERT INTO ${tableName} VALUES(3000, 2) """
    sql """ INSERT INTO ${tableName} VALUES(4000, 3) """

    sql """ DELETE FROM ${tableName} WHERE col1 < 3000 """

    sql """ INSERT INTO ${tableName} VALUES(5000, 4) """

    sql """ ALTER TABLE ${tableName} RENAME COLUMN col1 loc1 """

    sql """ INSERT INTO ${tableName} VALUES(6000, 5) """

    sql """ DELETE FROM ${tableName} WHERE col1 > 3000 """

    sql """ INSERT INTO ${tableName} VALUES(7000, 6) """

    sql """ ALTER TABLE ${tableName} RENAME COLUMN loc1 col1 """

    sql """ INSERT INTO ${tableName} VALUES(8000, 7) """

    sql """ DELETE FROM ${tableName} WHERE col1 < 6000 """

    def ret = sql """ SELECT COUNT(*) FROM ${tableName} """
    assertEquals(ret[0][0], 2)
}