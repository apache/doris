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

suite("test_split_by_regexp") {
    qt_select1 "select split_by_regexp('abcde','');"
    qt_select2 "select split_by_regexp('a12bc23de345f','\\\\d+');"
    qt_select3 "select split_by_regexp('a12bc23de345f',NULL);"
    qt_select4 "select split_by_regexp(NULL, 'a12bc23de345f');"

    def tableName1 = "test_split_by_regexp"

    sql """DROP TABLE IF EXISTS ${tableName1}"""
    sql """ 
            CREATE TABLE IF NOT EXISTS ${tableName1} (
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
    sql """ INSERT INTO ${tableName1} VALUES(1, 'abcde', '') """
    sql """ INSERT INTO ${tableName1} VALUES(2, '12553', '') """
    sql """ INSERT INTO ${tableName1} VALUES(3, '', '') """
    sql """ INSERT INTO ${tableName1} VALUES(4, '', ',') """
    sql """ INSERT INTO ${tableName1} VALUES(5, '', 'a') """
    sql """ INSERT INTO ${tableName1} VALUES(6, 'a1b1c1d', '1') """
    sql """ INSERT INTO ${tableName1} VALUES(7, ',,,', ',') """
    sql """ INSERT INTO ${tableName1} VALUES(8, 'a,b,c', ',') """
    sql """ INSERT INTO ${tableName1} VALUES(9, 'a,b,c,', ',') """
    sql """ INSERT INTO ${tableName1} VALUES(10, null, ',') """
    sql """ INSERT INTO ${tableName1} VALUES(11, 'a,b,c,12345,', ',') """

    test {
        sql " select split_by_regexp(NULL, 'a12bc23de345f', k1) from test_split_by_regexp"
        exception "function must be a positive constant"
    }
    test {
        sql " select split_by_regexp(NULL, 'a12bc23de345f', -10) from test_split_by_regexp"
        exception "function must be a positive constant"
    }
    test {
        sql " select split_by_regexp(NULL, 'a12bc23de345f', 1 + 2) from test_split_by_regexp"
        exception "function must be a positive constant"
    }
    qt_select5 "select split_by_regexp(v1, ',') from test_split_by_regexp order by k1;"
    qt_select6 "select split_by_regexp('do,ris', v2) from test_split_by_regexp order by k1;"
    qt_select7 "select split_by_regexp(v1, v2) from test_split_by_regexp order by k1;"
}

