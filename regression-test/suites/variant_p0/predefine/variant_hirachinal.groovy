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

suite("regression_test_variant_predefine_hirachinal", "variant_type"){
    def table_name = "var_rs"
    sql "DROP TABLE IF EXISTS ${table_name}"
    int count = new Random().nextInt(10) + 1

    sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant<'a' : largeint, 'c.d' : text, properties("variant_max_subcolumns_count" = "${count}")>
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    sql """insert into ${table_name} values (-3, '{"a" : 1, "b" : 1.5, "c" : [1, 2, 3]}')"""
    sql """insert into  ${table_name} select -2, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : "null", "e" : 7.111}}'  as json_str
            union  all select -1, '{"a": 1123}' as json_str union all select *, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
    qt_sql "select * from ${table_name} order by k limit 10"
    qt_sql "select cast(v['c'] as string) from ${table_name} where k = -3 or k = -2 order by k"
    qt_sql "select v['b'] from ${table_name} where k = -3 or k = -2"
    sql """insert into ${table_name} values (-3, '{"c" : 12345}')"""
    order_qt_sql1 "select cast(v['c'] as string) from var_rs where k = -3 or k = -2 or k = -4 or (k = 1 and v['c'] = 1024) order by k"
    order_qt_sql2 "select cast(v['c'] as string) from var_rs where k = -3 or k = -2 or k = 1 order by k, cast(v['c'] as text) limit 3"
}