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

suite("test_compatible_for_decimalv2", "nonConcurrent") {
    // create table with decimalv2 type
    sql """ admin set frontend config ("disable_decimalv2"="false"); """
    sql """ admin set frontend config ("enable_decimal_conversion"="false"); """
    sql "drop table if exists uniq_a"
    sql """ create table uniq_a(k1 int, v1 decimal(15,4)) unique key(k1) DISTRIBUTED by hash(k1) buckets 1 properties("replication_num"="1"); """

    // create table with decimalv3 type
    sql """ admin set frontend config ("disable_decimalv2"="true"); """
    sql """ admin set frontend config ("enable_decimal_conversion"="true"); """
    sql "drop table if exists uniq_b"
    sql """ create table uniq_b(k1 int, v1 decimal(27,8)) unique key(k1) DISTRIBUTED by hash(k1) buckets 1 properties("replication_num"="1"); """

    sql "insert into uniq_a values(1, '1.2345');"
    sql "insert into uniq_b values(1, '1.2345');"
    sql "sync"

    // using having + sum
    sql 'set enable_nereids_planner=false'
    qt_sql1 "select uniq_a.k1 from uniq_a left join uniq_b on uniq_a.k1 = uniq_b.k1 group by uniq_a.k1 having sum(uniq_b.v1) <> sum(uniq_a.v1);"
    qt_sql2 "select uniq_a.k1 from uniq_a left join uniq_b on uniq_a.k1 = uniq_b.k1 group by uniq_a.k1 having sum(uniq_b.v1) = sum(uniq_a.v1);"

    sql 'set enable_nereids_planner=true'
    qt_sql3 "select uniq_a.k1 from uniq_a left join uniq_b on uniq_a.k1 = uniq_b.k1 group by uniq_a.k1 having sum(uniq_b.v1) <> sum(uniq_a.v1);"
    qt_sql4 "select uniq_a.k1 from uniq_a left join uniq_b on uniq_a.k1 = uniq_b.k1 group by uniq_a.k1 having sum(uniq_b.v1) = sum(uniq_a.v1);"

    sql "drop table if exists uniq_a"
    sql "drop table if exists uniq_b"
}
