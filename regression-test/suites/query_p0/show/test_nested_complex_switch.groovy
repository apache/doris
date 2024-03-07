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

suite("test_nested_complex_switch", "query") {
    // define a sql table
    def testTable_m = "test_nested_complex_switch_map"
    def testTable_a = "test_nested_complex_switch_array"
    def testTable_s = "test_nested_complex_switch_struct"


    def sql_m_s = """CREATE TABLE IF NOT EXISTS ${testTable_m} (
                                           `k1` INT(11) NULL,
                                           `k2` MAP<STRING,STRUCT<F1:TINYINT(4)>>
                                         ) ENGINE=OLAP
                                         DUPLICATE KEY(`k1`)
                                         COMMENT 'OLAP'
                                         DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                                         PROPERTIES (
                                         "replication_allocation" = "tag.location.default: 1",
                                         "in_memory" = "false",
                                         "storage_format" = "V2",
                                         "disable_auto_compaction" = "false"
                                         )"""
    def sql_m_a = """CREATE TABLE IF NOT EXISTS ${testTable_m} (
                          `k1` INT(11) NULL,
                          `k2` MAP<ARRAY<INT>, STRING>
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`k1`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2",
                        "disable_auto_compaction" = "false"
                        )"""

    def sql_m_m = """CREATE TABLE IF NOT EXISTS ${testTable_m} (
                          `k1` INT(11) NULL,
                          `k2` MAP<STRING,MAP<STRING, INT>>
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`k1`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2",
                        "disable_auto_compaction" = "false"
                        )"""

    def sql_a_s = """CREATE TABLE IF NOT EXISTS ${testTable_a} (
                          `k1` INT(11) NULL,
                          `k2` ARRAY<STRUCT<F1:TINYINT(4)>>
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`k1`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2",
                        "disable_auto_compaction" = "false"
                        )"""

    def sql_a_m = """CREATE TABLE IF NOT EXISTS ${testTable_a} (
                          `k1` INT(11) NULL,
                          `k2` ARRAY<MAP<STRING, INT>>
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`k1`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2",
                        "disable_auto_compaction" = "false"
                        )"""

    def sql_s_s = """CREATE TABLE IF NOT EXISTS ${testTable_s} (
                          `k1` INT(11) NULL,
                          `k2` STRUCT<F1:STRUCT<F11:BOOLEAN>,F2:TINYINT(4),F3:ARRAY<INT(11)>>
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`k1`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2",
                        "disable_auto_compaction" = "false"
                        )"""

    def sql_s_a = """CREATE TABLE IF NOT EXISTS ${testTable_s} (
                          `k1` INT(11) NULL,
                          `k2` STRUCT<F2:TINYINT(4),F3:ARRAY<INT(11)>>
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`k1`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2",
                        "disable_auto_compaction" = "false"
                        )"""

    def sql_s_m = """CREATE TABLE IF NOT EXISTS ${testTable_s} (
                          `k1` INT(11) NULL,
                          `k2` STRUCT<F1:TINYINT(4), F2:MAP<BOOLEAN,TINYINT(4)>>
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`k1`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "in_memory" = "false",
                        "storage_format" = "V2",
                        "disable_auto_compaction" = "false"
                        )"""

    try {
        sql "DROP TABLE IF EXISTS ${testTable_m}"
        sql "DROP TABLE IF EXISTS ${testTable_a}"
        sql "DROP TABLE IF EXISTS ${testTable_s}"



        // map
        test {
            sql sql_m_s
        }

        test {
            sql sql_m_a
        }

        test {
            sql sql_m_m
        }

        // array
        test {
            sql sql_a_s
        }


        test {
            sql sql_a_m
        }

        // struct
        test {
            sql sql_s_s
        }

        test {
            sql sql_s_a
        }

        test {
            sql sql_s_m
        }

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable_m}")
        try_sql("DROP TABLE IF EXISTS ${testTable_a}")
        try_sql("DROP TABLE IF EXISTS ${testTable_s}")
    }

}
