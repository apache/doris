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
suite("test_pk_no_need_read_data", "p0"){
    def table1 = "test_pk_no_need_read_data"

    sql "drop table if exists ${table1}"

    sql """
       CREATE TABLE IF NOT EXISTS `${table1}` (
      `date` date NULL COMMENT "",
      `city` varchar(20) NULL COMMENT "",
      `addr` varchar(20) NULL COMMENT "",
      `name` varchar(20) NULL COMMENT "",
      `compy` varchar(20) NULL COMMENT "",
      `n` int NULL COMMENT "",
      INDEX idx_city(city) USING INVERTED,
      INDEX idx_addr(addr) USING INVERTED PROPERTIES("parser"="english"),
      INDEX idx_n(n) USING INVERTED
    ) ENGINE=OLAP
    DUPLICATE KEY(`date`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`date`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values
            ('2017-10-01',null,'addr qie3','yy','lj',100),
            ('2018-10-01',null,'hehe',null,'lala',200),
            ('2019-10-01','beijing','addr xuanwu','wugui',null,300),
            ('2020-10-01','beijing','addr fengtai','fengtai1','fengtai2',null),
            ('2021-10-01','beijing','addr chaoyang','wangjing','donghuqu',500),
            ('2022-10-01','shanghai','hehe',null,'haha',null),
            ('2023-10-01','tengxun','qie','addr gg','lj',null),
            ('2024-10-01','tengxun2','qie',null,'lj',800)
    """

    // case1: enable count on index
    sql "set enable_count_on_index_pushdown = true"

    qt_select_0 "SELECT COUNT() FROM ${table1} WHERE date='2017-10-01'"
    qt_select_1 "SELECT COUNT() FROM ${table1} WHERE year(date)='2017'"

    // case1: disable count on index
    sql "set enable_count_on_index_pushdown = false"

    qt_select_2 "SELECT COUNT() FROM ${table1} WHERE date='2017-10-01'"
    qt_select_3 "SELECT COUNT() FROM ${table1} WHERE year(date)='2017'"
}