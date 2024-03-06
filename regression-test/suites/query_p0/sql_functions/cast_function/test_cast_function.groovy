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

suite("test_cast_function") {
    qt_sql """ select cast (1 as BIGINT) """
    qt_sql """ select cast(cast ("11.2" as double) as bigint) """
    qt_sql """ select cast ("0.0101031417" as datetime) """
    qt_sql """ select cast ("0.0000031417" as datetime) """
    qt_sql """ select cast (NULL AS CHAR(1)); """
    qt_sql """ select cast ('20190101' AS CHAR(2)); """
    qt_sql """ select cast(cast(10000.00001 as double) as string); """
    qt_sql """ select cast('123.123' as float); """

    def tableName = "test_cast_function_nullable"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
        `uid` int(11) NULL COMMENT "",
        `datetimev2` datetimev2(3) NULL COMMENT "",
        `datetimev2_str` varchar(30) NULL COMMENT "",
        `datev2_val` datev2 NULL COMMENT "",
        `datev2_str` varchar(30) NULL COMMENT "",
        `date_val` date NULL COMMENT "",
        `date_str` varchar(30) NULL COMMENT "",
        `decimalv2_val` decimal(9,3) NULL COMMENT "",
        `decimalv2_str` varchar(30) NULL COMMENT "",
        `decimalv3_val` decimalv3(12,5) NULL COMMENT "",
        `decimalv3_str` varchar(30) NULL COMMENT ""
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
    (0,"2022-12-01 22:23:24.123",'2022-12-01 22:23:24.123','2022-12-01','2022-12-01','2022-12-01','2022-12-01','78.123','78.123','78.12345','78.12345'),
    (1,"2022-12-01 22:23:24.123456789",'2022-12-01 22:23:24.123456789','2022-12-01','2022-12-01','2022-12-01','2022-12-01','78.123','78.123','78.12345','78.12345'),
    (2,"2022-12-01 22:23:24.12341234",'2022-12-01 22:23:24.12341234','2022-12-01','2022-12-01','2022-12-01','2022-12-01','78.123','78.123','78.12341','78.12341')
    """

    qt_select1 "select * from ${tableName} order by uid"
    // test cast date,datetimev2,decimalv2,decimalv3 to string
    qt_select2 "select uid, datetimev2, cast(datetimev2 as string), datev2_val, cast(datev2_val as string), date_val, cast(date_val as string), decimalv2_val, cast(decimalv2_val as string),  decimalv3_val, cast(decimalv3_val as string) from ${tableName}  order by uid"
    // test cast from string to date,datetimev2,decimalv2,decimalv3
    qt_select3 "select uid, datetimev2_str, cast(datetimev2_str as datetimev2(5)), datev2_str, cast(datev2_str as datev2), date_str, cast(date_str as date), decimalv2_str, cast(decimalv2_str as decimal(9,5)),  decimalv3_str, cast(decimalv3_str as decimalv3(12,6)) from ${tableName}  order by uid"
}

