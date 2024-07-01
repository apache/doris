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

suite("test_inverted_index_null_arr", "array_contains_inverted_index") {
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=true"""
    sql """ set enable_common_expr_pushdown=true """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true """

    def tableName = "test_inverted_index_null_arr"

    sql "drop table if exists ${tableName}"

    sql """
       CREATE TABLE IF NOT EXISTS `${tableName}` (
      `id` int NULL COMMENT "",
      `city` array<varchar(20)> NULL COMMENT "[]",
      `addr` array<varchar(20)> NULL COMMENT "[]",
      `name` varchar(20) NULL COMMENT "",
      `compy` varchar(20) NULL COMMENT "",
      `n` array<int> NULL COMMENT "[]",
      INDEX idx_city(city) USING INVERTED,
      INDEX idx_addr(addr) USING INVERTED PROPERTIES("parser"="english"),
      INDEX idx_n(n) USING INVERTED
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """


        sql """insert into ${tableName} values
            (1,null,['addr qie3'],'yy','lj',[100]),
            (2,null,['hehe'],null,'lala',[200]),
            (3,['beijing'],['addr xuanwu'],'wugui',null,[300]),
            (4,['beijing'],['addr fengtai'],'fengtai1','fengtai2',null),
            (5,['beijing'],['addr chaoyang'],'wangjing','donghuqu',[500]),
            (6,['shanghai'],[null],null,'haha',[null]),
            (7,['tengxun'],['qie'],'addr gg','lj',[null]),
            (8,['tengxun2'],['null'],null,'lj',[800])
    """

        // select all data
        qt_select_0 "SELECT * FROM ${tableName} ORDER BY id"

        // test IS NULL , IS NOT NULL
        qt_select_is_null_1 "SELECT * FROM ${tableName} WHERE city IS NULL ORDER BY id"
        qt_select_is_null_2 "SELECT * FROM ${tableName} WHERE city IS NOT NULL ORDER BY id"
        qt_select_is_null_3 "SELECT * FROM ${tableName} WHERE addr IS NULL ORDER BY id"
        qt_select_is_null_4 "SELECT * FROM ${tableName} WHERE addr IS NOT NULL ORDER BY id"
        qt_select_is_null_5 "SELECT * FROM ${tableName} WHERE n IS NULL ORDER BY id"
        qt_select_is_null_6 "SELECT * FROM ${tableName} WHERE n IS NOT NULL ORDER BY id"

        // test array element IS NULL , IS NOT NULL
        qt_select_is_null_7 "SELECT * FROM ${tableName} WHERE city[1] IS NULL ORDER BY id"
        qt_select_is_null_8 "SELECT * FROM ${tableName} WHERE city[1] IS NOT NULL ORDER BY id"
        qt_select_is_null_9 "SELECT * FROM ${tableName} WHERE addr[1] IS NULL ORDER BY id"
        qt_select_is_null_10 "SELECT * FROM ${tableName} WHERE addr[1] IS NOT NULL ORDER BY id"
        qt_select_is_null_11 "SELECT * FROM ${tableName} WHERE n[1] IS NULL ORDER BY id"
        qt_select_is_null_12 "SELECT * FROM ${tableName} WHERE n[1] IS NOT NULL ORDER BY id"


        // test compare predicate
        qt_select_compare_11 "SELECT * FROM ${tableName} WHERE array_contains(city, 'shanghai') ORDER BY id"
        qt_select_compare_12 "SELECT * FROM ${tableName} WHERE !array_contains(city, 'shanghai') ORDER BY id"
        qt_select_compare_13 "SELECT * FROM ${tableName} WHERE city[1] <= 'shanghai' ORDER BY id"
        qt_select_compare_14 "SELECT * FROM ${tableName} WHERE city[1] >= 'shanghai' ORDER BY id"

        qt_select_compare_21 "SELECT * FROM ${tableName} WHERE array_contains(n, 500) ORDER BY id"
        qt_select_compare_22 "SELECT * FROM ${tableName} WHERE !array_contains(n, 500) ORDER BY id"

        qt_select_compare_23 "SELECT * FROM ${tableName} WHERE n[1] <= 500 ORDER BY id"
        qt_select_compare_24 "SELECT * FROM ${tableName} WHERE n[1] >= 500 ORDER BY id"

        // test match predicates
        qt_select_match_1 "SELECT * FROM ${tableName} WHERE array_contains(addr, 'addr fengtai') ORDER BY id"
        qt_select_match_2 "SELECT * FROM ${tableName} WHERE array_contains(addr, 'addr') ORDER BY id"
}

