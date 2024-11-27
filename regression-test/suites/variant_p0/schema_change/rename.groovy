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

suite("regression_test_variant_column_rename", "variant_type"){
    sql "DROP TABLE IF EXISTS variant_renam"
    sql """
        CREATE TABLE IF NOT EXISTS variant_renam(
            k bigint not null,
            v variant not null
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1");
    """

    sql """INSERT INTO variant_renam SELECT *, '{"k1":1, "k2": "hello world", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "1")"""
    sql """alter table variant_renam rename column v va""";
    qt_sql """select * from variant_renam"""

    // drop column and add the same name column
    sql """alter table variant_renam add column v2 variant default null"""
    sql """insert into variant_renam values (2, '{"xxxx" :  1234}', '{"yyyy" : 1.1111}')"""
    qt_sql "select * from variant_renam order by k"
    sql """alter table variant_renam drop column v2"""
    sql """insert into variant_renam values (2, '{"xxxx" :  1234}')"""
    sql """alter table variant_renam add column v2 variant default null"""
    qt_sql "select * from variant_renam order by k"
}