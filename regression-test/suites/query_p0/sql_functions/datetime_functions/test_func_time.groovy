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

suite("test_time") {
    qt_sql_const """ select time('2025-1-1 12:12:12') """
    qt_const_null """ select time(null) """
    qt_const_nullable_not_null """ select time(nullable('2025-1-1 12:12:12')) """
    qt_sql_wrong_input1 """ select time('2025-1-1 12:12:61') """
    qt_sql_wrong_input2 """ select time('2025-1-1 12:61:12') """
    qt_sql_wrong_input3 """ select time('2025-1-1 25:12:12') """
    qt_sql_wrong_input4 """ select time('2025-1-32 12:12:12') """
    qt_sql_wrong_input5 """ select time('2025-13-1 12:12:12') """

    def tableName = "test_time_function"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k int,
                time_null datetimev2 NULL,
                time_not_null datetimev2 NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
            )
        """

    qt_empty_nullable "select time(time_null) from ${tableName}"
    qt_empty_not_nullable "select time(time_not_null) from ${tableName}"

    sql """ insert into ${tableName} values 
                (1, "2025-1-1 13:21:03", "2025-1-1 13:21:03"),
                (2, "2025-1-1 13:22:03", "2025-1-1 13:22:03"),
                (3, "2025-1-1 14:21:03", "2025-1-1 14:21:03"),
                (4, null, "2025-1-1 14:21:04");
    """
    qt_sql_time_null "select time(time_null) from ${tableName}"
    qt_sql_time_not_null "select time(time_not_null) from ${tableName}"
}
