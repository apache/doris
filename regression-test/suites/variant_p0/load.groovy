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

suite("regression_test_variant", "variant_type"){
    // prepare test table
    def table_name = "simple_variant_type"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY RANDOM BUCKETS 5 
            properties("replication_num" = "1", "disable_auto_compaction" = "true");
        """
    sql """insert into ${table_name} values (1,  '[1]'),(1,  '{"a" : 1}');"""
    sql """insert into ${table_name} values (1,  '[2]'),(1,  '{"a" : [[[1]]]}');"""
    sql """insert into ${table_name} values (1,  '3'),(1,  '{"a" : 1}'), (1,  '{"a" : [1]}');"""
    sql """insert into ${table_name} values (1,  '"4"'),(1,  '{"a" : "1223"}');"""
    sql """insert into ${table_name} values (1,  '5.0'),(1,  '{"a" : [1]}');"""
    sql """insert into ${table_name} values (1,  '"[6]"'),(1,  '{"a" : ["1", 2, 1.1]}');"""
    sql """insert into ${table_name} values (1,  '7'),(1,  '{"a" : 1, "b" : {"c" : 1}}');"""
    sql """insert into ${table_name} values (1,  '8.11111'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${table_name} values (1,  '"9999"'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${table_name} values (1,  '1000000'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${table_name} values (1,  '[123.0]'),(1,  '{"a" : 1, "b" : {"c" : 1}}'),(1,  '{"a" : 1, "b" : 10}');"""
    sql """insert into ${table_name} values (1,  '[123.2]'),(1,  '{"a" : 1, "b" : 10}'),(1,  '{"a" : 1, "b" : {"c" : 1}}');"""
    qt_sql "select count() from ${table_name}"
}
