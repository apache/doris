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


suite("test_unique_seq_map") {
    def tableName = "test_unique_seq_map"
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            CREATE TABLE `$tableName` (
                `a` bigint(20) NULL COMMENT "",
                `b` int(11) NULL COMMENT "",
                `c` int(11) NULL COMMENT "",
                `d` int(11) NULL COMMENT "",
                `e` int(11) NULL COMMENT "",
                `s1` int(11) NULL COMMENT "",
                `s2` int(11) NULL COMMENT ""
                ) ENGINE=OLAP
                UNIQUE KEY(`a`, `b`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
                PROPERTIES (
                "enable_unique_key_merge_on_write" = "false",
                "replication_num" = "1",
                "sequence_mapping.s1" = "c,d",
                "sequence_mapping.s2" = "e"
                );
        """

        sql "insert into $tableName(a, b, c, d, s1) values (1,1,4,4,4), (1,1,2,2,2), (1,1,3,3,3);"
        qt_sql "select * from $tableName;"
        qt_sql "select a, b, c from $tableName;"
        qt_sql "select a, b, c, s1 from $tableName;"
        sql "insert into $tableName(a, b, c, d, s1) values (1,1,1,1,1);"
        qt_sql "select * from $tableName;"
        qt_sql "select a, b, c, s1 from $tableName;"
        sql "insert into $tableName(a, b, e, s2) values (1,1,2,2);"
        qt_sql "select * from $tableName;"
        sql "insert into $tableName(a, b, e, s2) values (1,1,3,3);"
        qt_sql "select * from $tableName;"
        qt_sql "select a, b, e from $tableName;"
        qt_sql "select a, b, e, s2 from $tableName;"
        sql "insert into $tableName(a, b, c, d, e, s1, s2) values (2,2,2,2,2,4,4);"
        sql "insert into $tableName(a, b, c, d, e, s1, s2) values (2,2,3,3,3,4,4);"
        qt_sql "select * from $tableName;"
        qt_sql "select a, b, c, d, e from $tableName;"
}

