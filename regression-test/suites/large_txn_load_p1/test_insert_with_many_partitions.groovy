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

suite("test_insert_with_many_parititions") {
    def tableName = 'test_insert_with_many_parititions'

    def makePartitionsStmt = { partitionNum ->
        def stmt = ""
        for (int i = 0; i < partitionNum; i++) {
            if (i == (partitionNum - 1)) {
                stmt += """partition `p${i}` values [("${i}"), ("${i + 1}"))"""
            } else {
                stmt += """partition `p${i}` values [("${i}"), ("${i + 1}")),\n\t\t\t"""
            }
        }
        return stmt
    }

    // [start, end)
    def makeInsertStmt = { start, end ->
        def stmt = ""
        for (int i = start; i < end; i++) {
            if (i == (end - 1)) {
                stmt += """(${i}, false, 1, null, "c5", "2023-09-01", null, "c8", 1.1234, "c10")\n"""
            } else {
                stmt += """(${i}, false, 1, null, "c5", "2023-09-01", null, "c8", 1.1234, "c10"),\n"""
            }
        }
        return stmt
    }

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
                `c1` bigint(20) not null,
                `c2` boolean REPLACE_IF_NOT_NULL null,
                `c3` tinyint(4) REPLACE_IF_NOT_NULL null,
                `c4` decimalv3(9, 3) REPLACE_IF_NOT_NULL null,
                `c5` char(36) REPLACE_IF_NOT_NULL null,
                `c6` date REPLACE_IF_NOT_NULL null,
                `c7` datetime REPLACE_IF_NOT_NULL null,
                `c8` varchar(64) REPLACE_IF_NOT_NULL null,
                `c9` double REPLACE_IF_NOT_NULL null,
                `c10` string REPLACE_IF_NOT_NULL null
            ) engine=olap
            AGGREGATE KEY(`c1`)
            PARTITION BY RANGE(`c1`)
                    (
                        ${makePartitionsStmt(1000)}
                    )
            DISTRIBUTED BY HASH(`c1`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
    """

    sql """ insert into ${tableName} values ${makeInsertStmt(0, 1000)};"""
    qt_sql """ select count(*) from ${tableName};"""
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
}
