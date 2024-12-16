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

suite("regression_test_variant_add_drop_column", "variant_type"){
    def table_name = "variant_add_drop_column"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1",  "bloom_filter_columns" = "v");
    """
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}')"""

    sql "alter table variant_add_drop_column add column v2 variant default null"
    sql "alter table variant_add_drop_column add column t1 datetime default null"
    sql "alter table variant_add_drop_column add column t2 datetime default null"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345234567,"b" : 2}', '{"xxx" : 1}', "2021-01-01 01:01:01", "2021-01-01 01:01:01")"""
    sql "alter table variant_add_drop_column add column i1 int default null"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}', '{"xxx" : 1}', "2021-01-01 01:01:01", "2021-01-01 01:01:01", 12345)"""
    sql "alter table variant_add_drop_column drop column t1"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}', '{"xxx" : 1}', "2021-01-01 01:01:01", 12345)"""
    sql "alter table variant_add_drop_column drop column t2"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}', '{"xxx" : 1}', 12345)"""
    sql "alter table variant_add_drop_column drop column i1"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}', '{"xxx" : 1}')"""
    sql "alter table variant_add_drop_column drop column v"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}')"""
    sql "alter table variant_add_drop_column add column v variant default null"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}', '{"a" : 12345,"b" : 2}')"""
    sql "alter table variant_add_drop_column add column v3 variant default null"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}', '{"a" : 12345,"b" : 2}', '{"a" : 12345,"b" : 2}')"""
    sql "alter table variant_add_drop_column drop column v"
    sql "alter table variant_add_drop_column drop column v2"
    sql """insert into variant_add_drop_column values (1, '{"a" : 12345,"b" : 2}')"""
}