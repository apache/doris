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
suite("test_index_no_need_read_data", "inverted_index_select"){
    def table1 = "test_index_no_need_read_data"

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

    // case1: enable nereids planner
    sql "set enable_nereids_planner = true"

    qt_select_nereids_0 "SELECT * FROM ${table1} ORDER BY id"
    qt_select_nereids_1 "SELECT count() FROM ${table1} WHERE n > 100"
    qt_select_nereids_2 "SELECT count() FROM ${table1} WHERE city = 'beijing'"
    qt_select_nereids_3 "SELECT count(*) FROM ${table1} WHERE city = 'beijing'"
    qt_select_nereids_4 "SELECT * FROM ${table1} WHERE city = 'beijing' ORDER BY id"
    qt_select_nereids_5 "SELECT city, addr, name FROM ${table1} WHERE city = 'beijing' ORDER BY id"
    qt_select_nereids_6 "SELECT addr, name FROM ${table1} WHERE city > 'beijing' ORDER BY city"
    qt_select_nereids_7 "SELECT addr, name FROM ${table1} WHERE city > 'beijing' ORDER BY id"
    qt_select_nereids_8 "SELECT upper(city), name FROM ${table1} WHERE city != 'beijing' ORDER BY id"
    qt_select_nereids_9 "SELECT length(addr), name FROM ${table1} WHERE city != 'beijing' ORDER BY id"
    qt_select_nereids_10 "SELECT addr, name FROM ( SELECT * from ${table1} WHERE city != 'beijing' ORDER BY id) t"
    qt_select_nereids_11 "SELECT addr, name, upper(city) FROM ( SELECT * from ${table1} WHERE city != 'beijing' ORDER BY id) t"
    qt_select_nereids_12 "SELECT sum(n) FROM ${table1} WHERE city = 'beijing' group by id ORDER BY id"

    // case2: disable nereids planner
    sql "set enable_nereids_planner = false"
    
    qt_select_0 "SELECT * FROM ${table1} ORDER BY id"
    qt_select_1 "SELECT count() FROM ${table1} WHERE n > 100"
    qt_select_2 "SELECT count() FROM ${table1} WHERE city = 'beijing'"
    qt_select_3 "SELECT count(*) FROM ${table1} WHERE city = 'beijing'"
    qt_select_4 "SELECT * FROM ${table1} WHERE city = 'beijing' ORDER BY id"
    qt_select_5 "SELECT city, addr, name FROM ${table1} WHERE city = 'beijing' ORDER BY id"
    qt_select_6 "SELECT addr, name FROM ${table1} WHERE city > 'beijing' ORDER BY city"
    qt_select_7 "SELECT addr, name FROM ${table1} WHERE city > 'beijing' ORDER BY id"
    qt_select_8 "SELECT upper(city), name FROM ${table1} WHERE city != 'beijing' ORDER BY id"
    qt_select_9 "SELECT length(addr), name FROM ${table1} WHERE city != 'beijing' ORDER BY id"
    qt_select_10 "SELECT addr, name FROM ( SELECT * from ${table1} WHERE city != 'beijing' ORDER BY id) t"
    qt_select_11 "SELECT addr, name, upper(city) FROM ( SELECT * from ${table1} WHERE city != 'beijing' ORDER BY id) t"
    qt_select_12 "SELECT sum(n) FROM ${table1} WHERE city = 'beijing' group by id ORDER BY id"
}