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

suite("test_cast_map_function", "query") {
    sql """ set enable_nereids_planner = true; """
    sql """ set enable_fallback_to_original_planner=false; """ 
    def tableName = "tbl_test_cast_map_function_nereids"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` Map<char(7), int(11)> NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    // insert into with implicit cast
    sql """ INSERT INTO ${tableName} VALUES(1, {"aa": 1, "b": 2, "1234567": 77}) """
    sql """ INSERT INTO ${tableName} VALUES(2, {"b":12, "123":7777}) """

    qt_select """ select * from ${tableName} order by k1; """

    qt_select " select cast({} as MAP<INT,INT>);"
    qt_select " select cast(map() as MAP<INT,INT>); "
    qt_sql1 "select cast(NULL as MAP<string,int>)"

    // literal NONSTRICT_SUPERTYPE_OF cast
    qt_sql2 "select cast({'':''} as MAP<String,INT>);"
    qt_sql3 "select cast({1:2} as MAP<String,INT>);"

    // select SUPERTYPE_OF cast
    qt_sql4 "select cast(k2 as map<varchar, bigint>) from ${tableName} order by k1;"

    // select NONSTRICT_SUPERTYPE_OF cast , this behavior is same with nested scala type
    qt_sql5 "select cast(k2 as map<char(2), smallint>) from ${tableName} order by k1;"
    qt_sql6 "select cast(k2 as map<char(1), tinyint>) from ${tableName} order by k1;"
    qt_sql7 "select cast(k2 as map<char, string>) from ${tableName} order by k1;"
    qt_sql8 "select cast(k2 as map<int, string>) from ${tableName} order by k1;"
    qt_sql9 "select cast(k2 as map<largeint, decimal>) from ${tableName} order by k1;"
    qt_sql10 "select cast(k2 as map<double, datetime>) from ${tableName} order by k1;"
}
