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

suite("regression_test_variant_multi_var", "variant_type"){
    def table_name = "multi_variants"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """INSERT INTO ${table_name} SELECT *, '{"k1":1, "k2": "hello world", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "101")"""
    sql """INSERT INTO ${table_name} SELECT *, '{"k7":123, "k8": "elden ring", "k9" : 1.1112, "k10" : [1.12], "k11" : ["moon"]}' FROM numbers("number" = "203") where number > 100"""
    sql """INSERT INTO ${table_name} SELECT *, '{"k7":123, "k8": "elden ring", "k9" : 1.1112, "k10" : [1.12], "k11" : ["moon"]}' FROM numbers("number" = "411") where number > 200"""
    sql "alter table ${table_name} add column v2 variant default null"
    sql """INSERT INTO ${table_name} select k, v, v from ${table_name}"""
    sql "alter table ${table_name} add column v3 variant default null"
    sql """INSERT INTO ${table_name} select k, v, v, v from ${table_name}"""
    sql "alter table ${table_name} add column ss string default null"
    sql """INSERT INTO ${table_name} select k, v, v, v, v from ${table_name}"""
    sql """DELETE FROM ${table_name} where k = 1"""
    qt_sql """select cast(v["k1"] as tinyint), cast(v2["k2"] as text), cast(v3["k3"] as string), cast(v["k7"] as tinyint), cast(v2["k8"] as text), cast(v3["k9"] as double) from ${table_name} order by k, 1, 2, 3, 4, 5, 6 limit 10"""
    qt_sql """select cast(v["k1"] as tinyint), cast(v2["k2"] as text), cast(v3["k3"] as string), cast(v["k7"] as tinyint), cast(v2["k8"] as text), cast(v3["k9"] as double) from ${table_name} where k > 200 order by k, 1, 2, 3, 4, 5, 6 limit 10"""
    qt_sql """select cast(v["k1"] as tinyint), cast(v2["k2"] as text), cast(v3["k3"] as string), cast(v["k7"] as tinyint), cast(v2["k8"] as text), cast(v3["k9"] as double) from ${table_name} where k > 300 order by k, 1, 2, 3, 4, 5, 6 limit 10"""

    sql "alter table ${table_name} add column v4 variant default null"
    for (int i = 0; i < 20; i++) {
        sql """insert into ${table_name}  values (1, '{"a" : 1}', '{"a" : 1}', '{"a" : 1}', '{"a" : 1}', '{"a" : 1}')"""
    }

    trigger_and_wait_compaction(table_name, "full")

    sql "set enable_condition_cache = true"
    sql "set enable_sql_cache = false"
    qt_sql_condition_cache1 """select count() from ${table_name} where cast(v3['k1'] as bigint) = 1 and cast(v2['k2'] as string) = 'hello world' """
    qt_sql_condition_cache2 """select count() from ${table_name} where cast(v3['k1'] as bigint) = 1 and cast(v2['k2'] as string) = 'hello world' """

    qt_sql_condition_cache3 """select count() from ${table_name} where cast(v3['k1'] as bigint) = 1 or cast(v2['k2'] as string) = 'hello world' """
    qt_sql_condition_cache4 """select count() from ${table_name} where cast(v3['k1'] as bigint) = 1 or cast(v2['k2'] as string) = 'hello world' """

    qt_sql_condition_cache5 """select count() from ${table_name} where cast(v3['k1'] as bigint) = 1 and cast(v3['k2'] as string) = 'hello world' """
    qt_sql_condition_cache6 """select count() from ${table_name} where cast(v3['k1'] as bigint) = 1 and cast(v3['k2'] as string) = 'hello world' """

    qt_sql_condition_cache7 """select count() from ${table_name} where cast(v2['k1'] as bigint) = 1 or cast(v2['k2'] as string) = 'hello world' """
    qt_sql_condition_cache8 """select count() from ${table_name} where cast(v2['k1'] as bigint) = 1 or cast(v2['k2'] as string) = 'hello world' """

    qt_sql_condition_cache9 """select count() from ${table_name} where cast(v2['k2'] as string) = 'hello world' and array_contains(cast(v3['k11'] as array<string>), 'moon') """
    qt_sql_condition_cache10 """select count() from ${table_name} where cast(v2['k2'] as string) = 'hello world' and array_contains(cast(v3['k11'] as array<string>), 'moon') """

    qt_sql_condition_cache11 """select count() from ${table_name} where cast(v2['k1'] as bigint) = 1 or cast(v2['k1'] as double) < 2.0 """
    qt_sql_condition_cache12 """select count() from ${table_name} where cast(v2['k1'] as bigint) = 1 or cast(v2['k1'] as double) < 2.0 """
}