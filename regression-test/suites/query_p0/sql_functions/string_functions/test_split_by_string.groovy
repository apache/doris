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

suite("test_split_by_string") {
    // split by char
    qt_sql "select split_by_string('abcde','');"
    qt_sql "select split_by_string('12553','');"
    qt_sql "select split_by_string('','');"
    qt_sql "select split_by_string('',',');"
    qt_sql "select split_by_string('','a');"

    qt_sql "select split_by_string('a1b1c1d','1');"
    qt_sql "select split_by_string(',,,',',');"
    qt_sql "select split_by_string('a,b,c,abcde',',');"
    qt_sql "select split_by_string(',,a,b,c,',',');"
    qt_sql "select split_by_string('null',',');"
    
    // split by string
    qt_sql "select split_by_string('1,,2,3,,4,5,,abcde', ',,');"
    qt_sql "select split_by_string('abcde','');"
    qt_sql "select split_by_string('','');"
    qt_sql "select split_by_string('',',');"
    qt_sql "select split_by_string('','a');"

    qt_sql "select split_by_string('1,,2,3,,,,,,4,5, abcde', ',,');"
    qt_sql "select split_by_string(',,,,',',,');"
    qt_sql "select split_by_string('a,,b,,c',',,');"
    qt_sql "select split_by_string('a,,b,,c,,',',,');"
    qt_sql "select split_by_string(',,a,,b,,c,,',',,');"
    qt_sql "select split_by_string('null',',');"

    def tableName1 = "test_split_by_char"

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

    qt_sql "SELECT *, split_by_string(v1, v2) FROM ${tableName1} ORDER BY k1"

    def tableName2 = "test_split_by_string"

    sql """DROP TABLE IF EXISTS ${tableName2}"""
    sql """ 
            CREATE TABLE IF NOT EXISTS ${tableName2} (
              `k1` int(11) NULL COMMENT "",
              `v1` varchar(50) NULL COMMENT "",
              `v2` varchar(10) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName2} VALUES(1, '1,,2,3,,4,5,,abcde', ',,') """
    sql """ INSERT INTO ${tableName2} VALUES(2, 'abcde','') """
    sql """ INSERT INTO ${tableName2} VALUES(3, '', '') """
    sql """ INSERT INTO ${tableName2} VALUES(4, '', ',') """
    sql """ INSERT INTO ${tableName2} VALUES(5, '', 'a') """
    sql """ INSERT INTO ${tableName2} VALUES(6, '1,,2,3,,,,,,4,5,,abcde', ',,') """
    sql """ INSERT INTO ${tableName2} VALUES(7, ',,,', ',') """
    sql """ INSERT INTO ${tableName2} VALUES(8, 'a,b,c', ',') """
    sql """ INSERT INTO ${tableName2} VALUES(9, 'a,b,c,', ',') """
    sql """ INSERT INTO ${tableName2} VALUES(10, null, ',') """


    qt_sql "SELECT *, split_by_string(v1, v2) FROM ${tableName2} ORDER BY k1"

    // Case where both of operator are column string is covered by above test. 
    sql """DROP TABLE IF EXISTS test_split_by_string_2"""
    sql """
            CREATE TABLE IF NOT EXISTS test_split_by_string_2 (
              `rid` INT NULL,
              `str` TEXT NULL,
              `vc`  VARCHAR(5) NULL,
              `chr`   CHAR(5) NULL,
              `txt` TEXT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`rid`)
            DISTRIBUTED BY HASH(`rid`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO test_split_by_string_2 
            VALUES (1, "", "", "", ""),
                   (2, "", "", "", ""),
                   (3, "a,b,c", "a,b,c", "a,b,c", "a,b,c"),
                   (4, "a,b,c", "a,b,c", "a,b,c", "a,b,c")
        """
    // Left operator is const, right operator is column string
    qt_sql_1 """ 
                SELECT rid, 
                      split_by_string("abc", str),
                      split_by_string("abc", vc),
                      split_by_string("abc", chr),
                      split_by_string("abc", txt)
                FROM test_split_by_string_2 WHERE rid=1 OR rid=2 ORDER BY rid;
              """
    // Left operator is column string, right operator is const
    qt_sql_2 """
                SELECT rid, 
                      split_by_string(str, ","),
                      split_by_string(vc, ","),
                      split_by_string(chr, ","),
                      split_by_string(txt, ",")
                FROM test_split_by_string_2 WHERE rid=3 OR rid=4 ORDER BY rid;
             """

    // Empty string
    qt_sql_3 """
                SELECT rid, 
                      split_by_string(str, ""),
                      split_by_string(vc, ""),
                      split_by_string(chr, ""),
                      split_by_string(txt, "")
                FROM test_split_by_string_2 ORDER BY rid;
             """
    qt_sql_4 """
                SELECT rid, 
                      split_by_string("", str),
                      split_by_string("", vc),
                      split_by_string("", chr),
                      split_by_string("", txt)
                FROM test_split_by_string_2 ORDER BY rid;
             """
}