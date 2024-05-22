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

suite("get_assignment_compatible_type") {
    sql 'set enable_nereids_planner=false'
    sql "drop table if exists test_decimal_boolean"
    sql """create table test_decimal_boolean (
        id int,
        c1 boolean,
        c2 tinyint
        ) duplicate key (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        ); """
    sql "insert into test_decimal_boolean values (1,1,1),(2,0,1);"
    sql "sync"
    sql "drop view if exists test_decimal_boolean_view;"
    // 0.0 = c1 is the test point
    sql "create view test_decimal_boolean_view as select id,c1,c2 from test_decimal_boolean where 0.0=c1 and c2 = 1.0;"
    sql "select * from test_decimal_boolean_view;"
    qt_test_sql "show create view test_decimal_boolean_view"

    sql "drop table if exists test_decimal_boolean_union"
    sql """create table test_decimal_boolean_union (
        id int,
        c1 boolean,
        c2 decimalv3(1,1)
        ) duplicate key (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
    sql "insert into test_decimal_boolean_union values(1,true,0.0),(1,false,0.0)"
    qt_test_union "select * from (select c1 from test_decimal_boolean_union union select c2 from test_decimal_boolean_union) a order by 1"
}