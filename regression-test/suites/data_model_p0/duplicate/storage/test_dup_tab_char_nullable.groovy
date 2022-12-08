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

suite("test_dup_tab_char_nullable") {

    def table1 = "test_dup_tab_char_nullable"

    sql "drop table if exists ${table1}"

    sql """
CREATE TABLE IF NOT EXISTS `${table1}` (
  `city` char(20) NULL COMMENT "",
  `name` char(20) NULL COMMENT "",
  `addr` char(20) NULL COMMENT "",
  `compy` char(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`city`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`city`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)

    """
    sql "set enable_vectorized_engine = false"

    sql """insert into ${table1} values
       ('a1','a2','a3','a4'),
       ('b1','b2','b3','b4'),
       ('c1','f1','d3','f4'),
       ('c1','c2','c3','c4'),
       ('d1','d2','d3','d4'),
       ('e1','e2','e3','e4'),
       (null,'e2',null,'e4')
"""
    sql "set enable_vectorized_engine = true"

    qt_read_single_column_1 "select city from ${table1} where city in ('a1','e1') order by city"
    qt_read_single_column_2 "select city from ${table1} where city not in ('a1','e1') order by city"
    qt_read_single_column_3 "select city from ${table1} where city='a1'"
    qt_read_single_column_4 "select city from ${table1} where city!='a1'"

    qt_read_multiple_column_1 "select * from ${table1} where city in ('a1','e1') order by city"
    qt_read_multiple_column_2 "select * from ${table1} where city not in ('a1','e1') order by city"
    qt_read_multiple_column_3 "select * from ${table1} where city='a1'"
    qt_read_multiple_column_4 "select * from ${table1} where city!='a1'"

    qt_key_is_null "select * from ${table1} where city is null"
    qt_key_is_not_null "select * from ${table1} where city is not null"
    qt_non_key_is_null "select * from ${table1} where addr is null"
    qt_non_key_is_not_null "select * from ${table1} where addr is not null"

    sql "drop table if exists ${table1}"

}