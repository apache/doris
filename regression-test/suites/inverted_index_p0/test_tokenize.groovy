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

import java.sql.SQLException

suite("test_tokenize"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "tokenize_test"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
    CREATE TABLE IF NOT EXISTS ${indexTblName}(
      `id`int(11)NULL,
      `c` text NULL,
      INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="chinese") COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES(
      "replication_allocation" = "tag.location.default: 1"
    );
    """
    
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    sql "INSERT INTO $indexTblName VALUES (1, '我来到北京清华大学'), (2, '我爱你中国'), (3, '人民可以得到更多实惠');"
    qt_sql "SELECT TOKENIZE(c, \"'parser'='chinese'\") FROM $indexTblName";
    qt_sql "SELECT TOKENIZE(c, \"'parser'='chinese'\") FROM $indexTblName WHERE c MATCH '人民'";

    def indexTblName2 = "tokenize_test2"

    sql "DROP TABLE IF EXISTS ${indexTblName2}"
    // create 1 replica table
    sql """
    CREATE TABLE IF NOT EXISTS ${indexTblName2}(
      `id`int(11)NULL,
      `c` text NULL,
      INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="unicode") COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES(
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "INSERT INTO $indexTblName2 VALUES (1, '我来到北京清华大学'), (2, '我爱你中国'), (3, '人民可以得到更多实惠'), (4, '陕西省西安市高新区创业大厦A座，我的手机号码是12345678901,邮箱是12345678@qq.com，,ip是1.1.1.1，this information is created automatically.');"
    qt_sql "SELECT TOKENIZE(c, \"'parser'='unicode'\") FROM $indexTblName2";

    def indexTblName3 = "tokenize_test3"

    sql "DROP TABLE IF EXISTS ${indexTblName3}"
    // create 1 replica table
    sql """
    CREATE TABLE IF NOT EXISTS ${indexTblName3}(
      `id`int(11)NULL,
      `c` text NULL,
      INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="unicode") COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES(
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "INSERT INTO $indexTblName3 VALUES (1, '我来到北京清华大学'), (2, '我爱你中国'), (3, '人民可以得到更多实惠'), (4, '陕西省西安市高新区创业大厦A座，我的手机号码是12345678901,邮箱是12345678@qq.com，,ip是1.1.1.1，this information is created automatically.');"
    qt_sql "SELECT TOKENIZE(c, \"'parser'='chinese','parser_mode'='fine_grained'\") FROM $indexTblName3";
    qt_sql "SELECT TOKENIZE(c, \"'parser'='chinese', 'parser_mode'='fine_grained'\") FROM $indexTblName3";

    qt_tokenize_sql """SELECT TOKENIZE('GET /images/hm_bg.jpg HTTP/1.0 test:abc=bcd','"parser"="unicode","char_filter_type" = "char_replace","char_filter_pattern" = "._=:,","char_filter_replacement" = " "');"""
    qt_tokenize_sql """SELECT TOKENIZE('GET /images/hm_bg.jpg HTTP/1.0 test:abc=bcd', '"parser"="unicode","char_filter_type" = "char_replace", "char_filter_pattern" = "._=:,", "char_filter_replacement" = " "');"""

    qt_tokenize_sql """SELECT TOKENIZE('华夏智胜新税股票A', '"parser"="unicode"');"""
    qt_tokenize_sql """SELECT TOKENIZE('华夏智胜新税股票A', '"parser"="unicode","stopwords" = "none"');"""

    try {
      sql """ SELECT TOKENIZE('华夏智胜新税股票A', '"parser"="eng"'); """
    } catch (SQLException e) {
      if (e.message.contains("E-6000")) {
        log.info("e message: {}", e.message)
      } else {
        throw e
      }
    }

    qt_tokenize_sql """SELECT TOKENIZE('华夏智胜新税股票A', '"parser"="icu"');"""
    qt_tokenize_sql """SELECT TOKENIZE('มนไมเปนไปตามความตองการมนมหมายเลขอยในเนอหา', '"parser"="icu"');"""
    qt_tokenize_sql """SELECT TOKENIZE('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111', '"parser"="icu"');"""
    qt_tokenize_sql """SELECT TOKENIZE('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111', '"parser"="unicode"');"""

    qt_tokenize_sql """SELECT TOKENIZE('Apache Doris是一个现代化的MPP数据库', '"parser"="ik","parser_mode"="ik_smart"');"""
    qt_tokenize_sql """SELECT TOKENIZE('北京大学计算机科学与技术系', '"parser"="ik","parser_mode"="ik_smart"');"""
    qt_tokenize_sql """SELECT TOKENIZE('中华人民共和国', '"parser"="ik","parser_mode"="ik_smart"');"""

    qt_tokenize_sql """SELECT TOKENIZE('Apache Doris是一个现代化的MPP数据库', '"parser"="ik","parser_mode"="ik_max_word"');"""
    qt_tokenize_sql """SELECT TOKENIZE('北京大学计算机科学与技术系', '"parser"="ik","parser_mode"="ik_max_word"');"""
    qt_tokenize_sql """SELECT TOKENIZE('中华人民共和国', '"parser"="ik","parser_mode"="ik_max_word"');"""

    qt_tokenize_sql """SELECT TOKENIZE('😊🚀👍测试特殊符号：@#¥%……&*（）', '"parser"="ik","parser_mode"="ik_max_word"');"""
    qt_tokenize_sql """SELECT TOKENIZE('High＆Low', '"parser"="ik","parser_mode"="ik_max_word"');"""
    qt_tokenize_sql """SELECT TOKENIZE('1･2', '"parser"="ik","parser_mode"="ik_max_word"');"""
    qt_tokenize_sql """SELECT TOKENIZE('abcşīabc', '"parser"="ik","parser_mode"="ik_max_word"');"""

    // Regression for the case where the first argument is a constant literal
    // while the second argument is a non-const column. The generic
    // PreparedFunctionImpl::default_implementation_for_constant_arguments path
    // only fires when all arguments are constant, so the example from the bug
    // description (`SELECT tokenize('hello', '"parser"="english"') FROM t`)
    // never actually reaches FunctionTokenize::execute_impl. To exercise the
    // left_const branch added by this fix we need a non-const second argument;
    // we get that by reading the properties string from a table column.
    //
    // Before the fix _do_tokenize / _do_tokenize_none would produce a
    // dest_column with only 1 row (because unpack_if_const unwrapped the
    // ColumnConst of the first arg to its inner 1-row data column), so the
    // outer block ended up with a row count of 1 instead of input_rows_count.
    def constArgTbl = "tokenize_const_first_arg"
    sql "DROP TABLE IF EXISTS ${constArgTbl}"
    sql """
    CREATE TABLE IF NOT EXISTS ${constArgTbl}(
      `id` int NULL,
      `parser_config` text NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES("replication_allocation" = "tag.location.default: 1");
    """
    sql """INSERT INTO ${constArgTbl} VALUES
           (1, '"parser"="english"'),
           (2, '"parser"="english"'),
           (3, '"parser"="english"'),
           (4, '"parser"="english"'),
           (5, '"parser"="english"');"""

    // Row count must match the source table's row count, not collapse to 1.
    def cntEnglish = sql """SELECT COUNT(*) FROM
        (SELECT tokenize('hello world', parser_config) AS t FROM ${constArgTbl}) sub;"""
    assertEquals(5L, cntEnglish[0][0])

    // Every row must carry the tokenized value (and they must all be equal,
    // since the source string is constant across rows).
    def rowsEnglish = sql """SELECT id, tokenize('hello world', parser_config) AS t
        FROM ${constArgTbl} ORDER BY id;"""
    assertEquals(5, rowsEnglish.size())
    def firstEnglishTok = rowsEnglish[0][1]
    assertTrue(firstEnglishTok != null && firstEnglishTok.toString().contains("hello"))
    assertTrue(firstEnglishTok.toString().contains("world"))
    for (int i = 0; i < rowsEnglish.size(); i++) {
        assertEquals(i + 1, rowsEnglish[i][0] as Integer)
        assertEquals(firstEnglishTok, rowsEnglish[i][1])
    }

    // Same exercise for the PARSER_NONE branch, which is a separate code path
    // inside execute_impl (the _do_tokenize_none early return).
    def constArgNoneTbl = "tokenize_const_first_arg_none"
    sql "DROP TABLE IF EXISTS ${constArgNoneTbl}"
    sql """
    CREATE TABLE IF NOT EXISTS ${constArgNoneTbl}(
      `id` int NULL,
      `parser_config` text NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES("replication_allocation" = "tag.location.default: 1");
    """
    sql """INSERT INTO ${constArgNoneTbl} VALUES
           (1, '"parser"="none"'),
           (2, '"parser"="none"'),
           (3, '"parser"="none"');"""

    def cntNone = sql """SELECT COUNT(*) FROM
        (SELECT tokenize('hello world', parser_config) AS t FROM ${constArgNoneTbl}) sub;"""
    assertEquals(3L, cntNone[0][0])

    def rowsNone = sql """SELECT id, tokenize('hello world', parser_config) AS t
        FROM ${constArgNoneTbl} ORDER BY id;"""
    assertEquals(3, rowsNone.size())
    def firstNoneTok = rowsNone[0][1]
    assertTrue(firstNoneTok != null && firstNoneTok.toString().contains("hello world"))
    for (int i = 0; i < rowsNone.size(); i++) {
        assertEquals(i + 1, rowsNone[i][0] as Integer)
        assertEquals(firstNoneTok, rowsNone[i][1])
    }
}
