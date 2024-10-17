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

suite("test_count_substrings") {
    // const
    qt_select1 "select count_substrings(NULL,NULL);"
    qt_select2 "select count_substrings('a12bc23de345f',NULL);"
    qt_select3 "select count_substrings(NULL, 'a12bc23de345f');"
    qt_select4 "select count_substrings('a12bc23de345f','2');"

    sql """DROP TABLE IF EXISTS test_count_substrings"""
    sql """ 
            CREATE TABLE IF NOT EXISTS test_count_substrings (
              `k1` int(11) NULL COMMENT "",
              `s1` varchar(20) NULL COMMENT "",
              `s2` varchar(20) NOT NULL COMMENT "",
              `p1` varchar(20) NULL COMMENT "",
              `p2` varchar(20) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    // empty
    qt_select4_empty "select count_substrings(s1,p1) from test_count_substrings;"
    qt_select5_empty "select count_substrings(s2,p2) from test_count_substrings;"
    qt_select6_empty "select count_substrings(s1,p2) from test_count_substrings;"
    qt_select7_empty "select count_substrings(s2,p1) from test_count_substrings;"


    sql """ INSERT INTO test_count_substrings VALUES(1, 'abcde', 'abcde', '', '') """
    sql """ INSERT INTO test_count_substrings VALUES(2, '', '', '', '') """
    sql """ INSERT INTO test_count_substrings VALUES(3, '', '','a','a') """
    sql """ INSERT INTO test_count_substrings VALUES(4, NULL, '', NULL,'') """
    sql """ INSERT INTO test_count_substrings VALUES(5, 'asdasd', 'asdasd','a','a') """
    sql """ INSERT INTO test_count_substrings VALUES(6, 'a1b1c1d', 'a1b1c1d','1','1') """
    sql """ INSERT INTO test_count_substrings VALUES(7, ',,,', ',,,','#','#') """
    sql """ INSERT INTO test_count_substrings VALUES(8, 'a,b,c', 'a,b,c','v','v') """
    sql """ INSERT INTO test_count_substrings VALUES(9, 'a,b,c,', 'a,b,c',NULL,'') """
    sql """ INSERT INTO test_count_substrings VALUES(10, NULL, '','asd','asd') """
    sql """ INSERT INTO test_count_substrings VALUES(11, 'a,b,c,12345', 'a,b,c,12345','5','5') """
    sql """ INSERT INTO test_count_substrings VALUES(12, 'a,b,c,12345', 'a,b,c,12345','a','a') """

    qt_select5_null_null "select s1,p1,count_substrings(s1, p1) from test_count_substrings order by k1;"
    qt_select6_null_not "select s1, p2,count_substrings(s1, p2) from test_count_substrings order by k1;"
    qt_select7_not_null "select s2, p1,count_substrings(s2, p1) from test_count_substrings order by k1;"
    qt_select8_not_not "select s2, p2,count_substrings(s2, p2) from test_count_substrings order by k1;"
}

