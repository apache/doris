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

suite("test_convert_timestamp_to_string") {
    qt_sql """ SELECT CONVERT(TIMESTAMP "2004-01-22 21:45:33", CHAR(4)); """
    qt_sql """ SELECT CONVERT(TIMESTAMP "2004-01-22 21:45:33", VARCHAR(4)); """
    qt_sql """ SELECT CONVERT(TIMESTAMP "2004-01-22 21:45:33", CHAR(16)); """
    qt_sql """ SELECT CONVERT(TIMESTAMP "2004-01-22 21:45:33", VARCHAR(16)); """
    qt_sql """ SELECT CONVERT(TIMESTAMP "2004-01-22 21:45:33", CHAR(36)); """
    qt_sql """ SELECT CONVERT(TIMESTAMP "2004-01-22 21:45:33", VARCHAR(36)); """

    def tableName = "test_conveert_function"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
        `uid` int(11) NULL COMMENT "",
        `datetimev2` datetimev2(3) NULL COMMENT ""
        ) ENGINE=OLAP
    DUPLICATE KEY(`uid`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`uid`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """INSERT INTO ${tableName} values
    (0,"2022-12-01 22:23:24.123"),
    (1,"2022-12-02 22:23:24.123456789"),
    (2,"2022-12-03 22:23:24.12341234")
    """

    qt_sql """ select CONVERT(datetimev2, CHAR(4)) from test_conveert_function; """
    qt_sql """ select CONVERT(datetimev2, VARCHAR(4)) from test_conveert_function; """
    qt_sql """ select CONVERT(datetimev2, CHAR(15)) from test_conveert_function; """
    qt_sql """ select CONVERT(datetimev2, VARCHAR(15)) from test_conveert_function; """
    qt_sql """ select CONVERT(datetimev2, CHAR(25)) from test_conveert_function; """
    qt_sql """ select CONVERT(datetimev2, VARCHAR(25)) from test_conveert_function; """
}

