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

suite("test_inverted_index_null") {

    def table1 = "test_inverted_index_null"

    sql "drop table if exists ${table1}"

    sql """
       CREATE TABLE IF NOT EXISTS `${table1}` (
      `id` int NULL COMMENT "",
      `city` varchar(20) NULL COMMENT "",
      `addr` varchar(20) NULL COMMENT "",
      `name` varchar(20) NULL COMMENT "",
      `compy` varchar(20) NULL COMMENT "",
      `n` int NULL COMMENT "",
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

    sql """insert into ${table1} values
            (1,null,'addr qie3','yy','lj',100),
            (2,null,'hehe',null,'lala',200),
            (3,'beijing','addr xuanwu','wugui',null,300),
            (4,'beijing','addr fengtai','fengtai1','fengtai2',null),
            (5,'beijing','addr chaoyang','wangjing','donghuqu',500),
            (6,'shanghai','hehe',null,'haha',null),
            (7,'tengxun','qie','addr gg','lj',null),
            (8,'tengxun2','qie',null,'lj',800)
    """
    sql """ set enable_common_expr_pushdown = true """

    // select all data
    qt_select_0 "SELECT * FROM ${table1} ORDER BY id"

    // test IS NULL , IS NOT NULL
    qt_select_is_null_1 "SELECT * FROM ${table1} WHERE city IS NULL ORDER BY id"
    qt_select_is_null_2 "SELECT * FROM ${table1} WHERE city IS NOT NULL ORDER BY id"
    qt_select_is_null_3 "SELECT * FROM ${table1} WHERE addr IS NULL ORDER BY id"
    qt_select_is_null_4 "SELECT * FROM ${table1} WHERE addr IS NOT NULL ORDER BY id"
    qt_select_is_null_5 "SELECT * FROM ${table1} WHERE n IS NULL ORDER BY id"
    qt_select_is_null_6 "SELECT * FROM ${table1} WHERE n IS NOT NULL ORDER BY id"

    // test compare predicate
    qt_select_compare_11 "SELECT * FROM ${table1} WHERE city  = 'shanghai' ORDER BY id"
    qt_select_compare_12 "SELECT * FROM ${table1} WHERE city != 'shanghai' ORDER BY id"
    qt_select_compare_13 "SELECT * FROM ${table1} WHERE city <= 'shanghai' ORDER BY id"
    qt_select_compare_14 "SELECT * FROM ${table1} WHERE city >= 'shanghai' ORDER BY id"

    qt_select_compare_21 "SELECT * FROM ${table1} WHERE n  = 500 ORDER BY id"
    qt_select_compare_22 "SELECT * FROM ${table1} WHERE n != 500 ORDER BY id"
    qt_select_compare_23 "SELECT * FROM ${table1} WHERE n <= 500 ORDER BY id"
    qt_select_compare_24 "SELECT * FROM ${table1} WHERE n >= 500 ORDER BY id"

    // test in predicates
    qt_select_in_1 "SELECT * FROM ${table1} WHERE city IN ('shanghai', 'beijing') ORDER BY id"
    qt_select_in_2 "SELECT * FROM ${table1} WHERE city NOT IN ('shanghai', 'beijing') ORDER BY id"
    qt_select_in_3 "SELECT * FROM ${table1} WHERE n IN (100, 300) ORDER BY id"
    qt_select_in_4 "SELECT * FROM ${table1} WHERE n NOT IN (100, 300) ORDER BY id"

    // test match predicates
    qt_select_match_1 "SELECT * FROM ${table1} WHERE addr MATCH_ANY 'addr fengtai' ORDER BY id"
    qt_select_match_2 "SELECT * FROM ${table1} WHERE addr MATCH_ALL 'addr fengtai' ORDER BY id"
}

