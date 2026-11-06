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

suite("dotted_key_escape_async_mv", "p0,nonConcurrent") {
    sql "set default_variant_enable_nested_group=false"
    sql "DROP TABLE IF EXISTS dotted_key_escape_async_source"
    sql """CREATE TABLE dotted_key_escape_async_source(k INT,v VARIANT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="true")"""
    sql """INSERT INTO dotted_key_escape_async_source VALUES
        (1,parse_to_variant('{"a.b":101,"a":{"b":102},"a%2Eb":103}'))"""
    sql "DROP TABLE IF EXISTS dotted_key_escape_async_legacy"
    sql """CREATE TABLE dotted_key_escape_async_legacy(k INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO dotted_key_escape_async_legacy VALUES (1)"
    sql "DROP MATERIALIZED VIEW IF EXISTS dotted_key_escape_async_inherited"
    sql "DROP MATERIALIZED VIEW IF EXISTS dotted_key_escape_async_true"
    sql "DROP MATERIALIZED VIEW IF EXISTS dotted_key_escape_async_false"
    sql """CREATE MATERIALIZED VIEW dotted_key_escape_async_inherited
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS WITH c AS (SELECT * FROM dotted_key_escape_async_source)
        SELECT k,coalesce(v,v) v FROM c"""
    sql """CREATE MATERIALIZED VIEW dotted_key_escape_async_true
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="true")
        AS SELECT k,parse_to_variant('{"a.b":201,"a":{"b":202}}') v FROM dotted_key_escape_async_legacy"""
    sql """CREATE MATERIALIZED VIEW dotted_key_escape_async_false
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="false")
        AS SELECT k,parse_to_variant('{"legacy.key":401}') v FROM dotted_key_escape_async_source"""
    def db = context.config.getDbNameByFile(context.file)
    ['dotted_key_escape_async_inherited','dotted_key_escape_async_true','dotted_key_escape_async_false'].each { name ->
        sql "REFRESH MATERIALIZED VIEW ${name} COMPLETE"
        waitingMTMVTaskFinished(getJobName(db,name))
    }
    quickRunTest("inherited_property", "SHOW CREATE MATERIALIZED VIEW dotted_key_escape_async_inherited", false,
            { row -> [row[1].contains('variant_escape_dots_in_keys').toString()] })
    quickRunTest("false_property", "SHOW CREATE MATERIALIZED VIEW dotted_key_escape_async_false", false,
            { row -> [row[1].contains('variant_escape_dots_in_keys').toString()] })
    qt_inherited """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'] FROM dotted_key_escape_async_inherited ORDER BY k"""
    qt_true """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_async_true ORDER BY k"""
    qt_false "SELECT k,v FROM dotted_key_escape_async_false ORDER BY k"
    sql """INSERT INTO dotted_key_escape_async_source VALUES
        (2,parse_to_variant('{"a.b":301,"a":{"b":302},"a%2Eb":303}'))"""
    sql "REFRESH MATERIALIZED VIEW dotted_key_escape_async_inherited COMPLETE"
    waitingMTMVTaskFinished(getJobName(db,'dotted_key_escape_async_inherited'))
    qt_refreshed """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'] FROM dotted_key_escape_async_inherited ORDER BY k"""
}
