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

suite("test_ik_analyzer", "p0") {
    def tableNameSmart = "test_ik_analyzer_smart"
    def tableNameMaxWord = "test_ik_analyzer_maxword"

    sql "DROP TABLE IF EXISTS ${tableNameSmart}"
    sql "DROP TABLE IF EXISTS ${tableNameMaxWord}"

    // Create test table for smart mode
    sql """
      CREATE TABLE ${tableNameSmart} (
      `id` int(11) NULL COMMENT "",
      `content` text NULL COMMENT "",
      INDEX content_idx (`content`) USING INVERTED PROPERTIES("parser" = "ik", "parser_mode" = "ik_smart") COMMENT '',
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    // Create test table for max_word mode
    sql """
      CREATE TABLE ${tableNameMaxWord} (
      `id` int(11) NULL COMMENT "",
      `content` text NULL COMMENT "",
      INDEX content_idx (`content`) USING INVERTED PROPERTIES("parser" = "ik", "parser_mode" = "ik_max_word") COMMENT '',
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    // Insert test data
    def insertData = { table ->
        sql """ INSERT INTO ${table} VALUES (1, "我爱北京天安门"); """
        sql """ INSERT INTO ${table} VALUES (2, "Apache Doris是一个现代化的MPP数据库"); """
        sql """ INSERT INTO ${table} VALUES (3, "中华人民共和国"); """
        sql """ INSERT INTO ${table} VALUES (4, "数据库管理系统"); """
        sql """ INSERT INTO ${table} VALUES (5, "北京大学计算机科学与技术系"); """
    }

    insertData(tableNameSmart)
    insertData(tableNameMaxWord)

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        // Testing ik smart mode
        println "Testing ik smart mode:"
        qt_sql """ select * from ${tableNameSmart} where content match_phrase '北京'; """
        qt_sql """ select * from ${tableNameSmart} where content match_phrase '计算机科学'; """
        qt_sql """ select * from ${tableNameSmart} where content match_phrase '数据库管理系统'; """
        qt_sql """ select * from ${tableNameSmart} where content match_phrase '中华人民共和国'; """

        // Testing ik max_word mode
        println "Testing ik max_word mode:"
        qt_sql """ select * from ${tableNameMaxWord} where content match_phrase '北京'; """
        qt_sql """ select * from ${tableNameMaxWord} where content match_phrase '计算机科学'; """
        qt_sql """ select * from ${tableNameMaxWord} where content match_phrase '数据库管理系统'; """
        qt_sql """ select * from ${tableNameMaxWord} where content match_phrase '中华人民共和国'; """

    } finally {
        sql "DROP TABLE IF EXISTS ${tableNameSmart}"
        sql "DROP TABLE IF EXISTS ${tableNameMaxWord}"
    }
}

