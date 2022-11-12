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

suite("test_split_by_char") {
    sql "set enable_vectorized_engine = true;"

    qt_sql "select split_by_char('abcde','');"
    qt_sql "select split_by_char('12553','');"
    qt_sql "select split_by_char('','');"
    qt_sql "select split_by_char('',',');"
    qt_sql "select split_by_char('','a');"

    qt_sql "select split_by_char('a1b1c1d','1');"
    qt_sql "select split_by_char(',,,',',');"
    qt_sql "select split_by_char('a,b,c',',');"
    qt_sql "select split_by_char('a,b,c,',',');"
    qt_sql "select split_by_char('null',',');"

    def tableName = "test_split_by_char"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """ 
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `v1` varchar(20) NULL COMMENT "",
              `v2` varchar(1) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, 'abcde', '') """
    sql """ INSERT INTO ${tableName} VALUES(2, '12553', '') """
    sql """ INSERT INTO ${tableName} VALUES(3, '', '') """
    sql """ INSERT INTO ${tableName} VALUES(4, '', ',') """
    sql """ INSERT INTO ${tableName} VALUES(5, '', 'a') """
    sql """ INSERT INTO ${tableName} VALUES(6, 'a1b1c1d', '1') """
    sql """ INSERT INTO ${tableName} VALUES(7, ',,,', ',') """
    sql """ INSERT INTO ${tableName} VALUES(8, 'a,b,c', ',') """
    sql """ INSERT INTO ${tableName} VALUES(9, 'a,b,c,', ',') """
    sql """ INSERT INTO ${tableName} VALUES(10, null, ',') """
    sql """ INSERT INTO ${tableName} VALUES(11, 'a,b,c,12345,', ',') """

    qt_sql "SELECT *, split_by_char(v1, v2) FROM ${tableName} ORDER BY k1"
}