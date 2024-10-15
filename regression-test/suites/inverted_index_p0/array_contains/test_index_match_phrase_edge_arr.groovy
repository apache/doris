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


suite("test_index_match_phrase_edge_arr", "array_contains_inverted_index"){
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=true"""
    sql """ set enable_common_expr_pushdown=true """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true """

    def indexTbName1 = "test_index_match_phrase_edge_arr"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `a` int(11) NULL COMMENT "",
      `b` array<text> NULL COMMENT "[]",
      `c` array<text> NULL COMMENT "[]",
      INDEX b_idx (`b`) USING INVERTED PROPERTIES("parser" = "none") COMMENT '',
      INDEX c_idx (`c`) USING INVERTED PROPERTIES("parser" = "none") COMMENT '',
      ) ENGINE=OLAP
      DUPLICATE KEY(`a`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql """ INSERT INTO ${indexTbName1} VALUES (1, ["index.html"], ["首先我 index html 想说的是这里有 index html 条评论看了之后很让人无语"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (2, ["nav_inet.html"], ["尤其看看 nav inet html 原价应当 nav inet html 是一本精美的书"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (3, ["splash_inet.html"], ["封面 splash inet html 红色 splash inet html 书封非常精致"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (4, ["nav_top_inet.html"], ["个人觉得定义 nav top inet html 和 nav top inet html 写法特别有帮助"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (5, ["nav_bg_top.gif"], ["该书研究了英语 nav bg top gif 各种语法 nav bg top gif 结构下的歧义问题"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (6, ["nav_news_off.gif"], ["作品当然是 nav news off gif 喜欢的 nav news off gif 否则也不会买原版"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (7, ["nav_comp_off.gif"], ["对于理解英语的 nav comp off gif 节奏和 nav comp off gif 韵律很有好处"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (8, ["nav_venue_off.gif"], ["本书既适合 nav venue off gif 家长 nav venue off gif 和孩子一起学习使用"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (9, ["hm_bg.jpg"], ["前几日 hm bg jpg 在别处 hm bg jpg 购得"]); """
    sql """ INSERT INTO ${indexTbName1} VALUES (10, ["nav_tickets_off.gif"], ["习惯于生活中很多 nav tickets off gif 虚假 nav tickets off gif 美化的人来说"]); """

    try {
        sql "sync"

        qt_sql """ select * from ${indexTbName1} where array_contains(b, 'x.h'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(b, 'v_i'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(b, 'sh_inet.h'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(b, 'v_bg_t'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(b, 'v_venue_of'); """

        qt_sql """ select * from ${indexTbName1} where array_contains(c, 'ml 想说的是这里有 in'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(c, 'ml 原价应当 na'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(c, 'op gif 各种语法 nav b'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(c, 'ue off gif 家长 na'); """
        qt_sql """ select * from ${indexTbName1} where array_contains(c, 'if 虚假 na'); """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}