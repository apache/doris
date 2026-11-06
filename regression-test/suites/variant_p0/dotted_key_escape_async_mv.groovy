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
    test {
        sql """CREATE MATERIALIZED VIEW dotted_key_escape_async_false
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="false")
            AS SELECT k,v FROM dotted_key_escape_async_source"""
        exception "async MV must use the same value of variant_escape_dots_in_keys"
    }
    sql "DROP TABLE IF EXISTS dotted_key_escape_async_legacy_variant"
    sql """CREATE TABLE dotted_key_escape_async_legacy_variant(k INT,v VARIANT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    sql "DROP MATERIALIZED VIEW IF EXISTS dotted_key_escape_async_mixed"
    test {
        sql """CREATE MATERIALIZED VIEW dotted_key_escape_async_mixed
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1") AS SELECT s.k,s.v FROM dotted_key_escape_async_source s
            JOIN dotted_key_escape_async_legacy_variant l ON s.k=l.k"""
        exception "async MV must use the same value of variant_escape_dots_in_keys"
    }
    sql "DROP MATERIALIZED VIEW IF EXISTS dotted_key_escape_async_int_join"
    sql """CREATE MATERIALIZED VIEW dotted_key_escape_async_int_join
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS SELECT s.k,s.v FROM dotted_key_escape_async_source s
        JOIN dotted_key_escape_async_legacy l ON s.k=l.k"""
    def db = context.config.getDbNameByFile(context.file)
    ['dotted_key_escape_async_inherited','dotted_key_escape_async_true','dotted_key_escape_async_int_join'].each { name ->
        sql "REFRESH MATERIALIZED VIEW ${name} COMPLETE"
        waitingMTMVTaskFinished(getJobName(db,name))
    }
    quickRunTest("inherited_property", "SHOW CREATE MATERIALIZED VIEW dotted_key_escape_async_inherited", false,
            { row -> [row[1].contains('variant_escape_dots_in_keys').toString()] })
    quickRunTest("int_join_property", "SHOW CREATE MATERIALIZED VIEW dotted_key_escape_async_int_join", false,
            { row -> [row[1].contains('variant_escape_dots_in_keys').toString()] })
    qt_inherited """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'] FROM dotted_key_escape_async_inherited ORDER BY k"""
    qt_true """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_async_true ORDER BY k"""
    qt_int_join """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_async_int_join ORDER BY k"""
    sql """INSERT INTO dotted_key_escape_async_source VALUES
        (2,parse_to_variant('{"a.b":301,"a":{"b":302},"a%2Eb":303}'))"""
    sql "REFRESH MATERIALIZED VIEW dotted_key_escape_async_inherited COMPLETE"
    waitingMTMVTaskFinished(getJobName(db,'dotted_key_escape_async_inherited'))
    qt_refreshed """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'] FROM dotted_key_escape_async_inherited ORDER BY k"""

    sql "DROP MATERIALIZED VIEW IF EXISTS dotted_key_escape_async_view_mv"
    sql "DROP VIEW IF EXISTS dotted_key_escape_async_view"
    sql """INSERT INTO dotted_key_escape_async_legacy_variant VALUES
        (1,parse_to_variant('{"a":{"b":701}}'))"""
    sql """CREATE VIEW dotted_key_escape_async_view AS
        SELECT k,v FROM dotted_key_escape_async_legacy_variant"""
    sql """CREATE MATERIALIZED VIEW dotted_key_escape_async_view_mv
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS SELECT k,v FROM dotted_key_escape_async_view"""
    sql "REFRESH MATERIALIZED VIEW dotted_key_escape_async_view_mv COMPLETE"
    waitingMTMVTaskFinished(getJobName(db,'dotted_key_escape_async_view_mv'))
    qt_view_before "SELECT k,v['a']['b'] FROM dotted_key_escape_async_view_mv ORDER BY k"
    sql """ALTER VIEW dotted_key_escape_async_view AS SELECT k,v FROM dotted_key_escape_async_source"""
    sql "REFRESH MATERIALIZED VIEW dotted_key_escape_async_view_mv COMPLETE"
    waitingMTMVTaskFinishedNotNeedSuccess(getJobName(db,'dotted_key_escape_async_view_mv'))
    qt_view_refresh_failed """SELECT Status,ErrorMsg LIKE '%async MV must use the same value of variant_escape_dots_in_keys%'
        FROM tasks('type'='mv') WHERE MvDatabaseName='${db}' AND MvName='dotted_key_escape_async_view_mv'
        ORDER BY CreateTime DESC LIMIT 1"""
    qt_view_not_rewrite_candidate """SELECT State,RefreshState FROM mv_infos('database'='${db}')
        WHERE Name='dotted_key_escape_async_view_mv'"""
    qt_view_after "SELECT k,v['a']['b'] FROM dotted_key_escape_async_view_mv ORDER BY k"
}
