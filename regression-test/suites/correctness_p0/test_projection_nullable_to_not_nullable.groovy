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

suite("test_projection_nullable_to_not_nullable") {
    def tableName = "test_projection_nullable_to_not_nullable_tbl"

    sql """DROP TABLE IF EXISTS ${tableName};"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k INT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1"
        );
    """

    sql """INSERT INTO ${tableName} SELECT * FROM numbers("number" = "10");"""
    qt_select """SELECT count(*) FROM ${tableName}; """

    test {
        sql """INSERT INTO ${tableName} SELECT IFNULL(NULL, NULL) AS col FROM numbers("number" = "10"); """
        exception "Insert has filtered data in strict mode"
    }

    sql """DROP TABLE IF EXISTS ${tableName};"""
}
