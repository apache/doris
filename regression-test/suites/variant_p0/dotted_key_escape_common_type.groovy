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

suite("dotted_key_escape_common_type", "p0") {
    sql "set group_commit='off_mode'"
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    sql "set enable_materialized_view_rewrite=false"
    sql "DROP TABLE IF EXISTS dotted_key_escape_common_type"
    sql """CREATE TABLE dotted_key_escape_common_type (
        k INT,
        v1 VARIANT<PROPERTIES("variant_escape_dots_in_keys"="true",
            "variant_max_subcolumns_count"="1", "variant_enable_nested_group"="false")>,
        v2 VARIANT<PROPERTIES("variant_escape_dots_in_keys"="true",
            "variant_max_subcolumns_count"="2", "variant_enable_nested_group"="false")>,
        legacy VARIANT<PROPERTIES("variant_escape_dots_in_keys"="false",
            "variant_enable_nested_group"="false")>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    // Both escaped inputs have different layouts. The legacy input also participates in the
    // common type, but must not make literal dotted keys from the escaped inputs become paths.
    sql """INSERT INTO dotted_key_escape_common_type VALUES
        (1,CAST(CAST('{"a.b":101,"a":{"b":102},"a%2Eb":103,"50%":104,"":105}' AS JSON) AS VARIANT),NULL,NULL),
        (2,NULL,CAST(CAST('{"a.b":201,"a":{"b":202},"a%2Eb":203,"50%":204,"":205}' AS JSON) AS VARIANT),NULL)"""
    def db = context.config.getDbNameByFile(context.file)
    create_sync_mv(db, 'dotted_key_escape_common_type', 'dotted_escape_common_mv',
            "SELECT k AS kk, coalesce(v1,v2,legacy) AS v FROM dotted_key_escape_common_type")
    // Exercise materialization after historical backfill, including a legacy branch.
    sql """INSERT INTO dotted_key_escape_common_type VALUES
        (3,NULL,NULL,CAST(CAST('{"a":{"b":302},"a%2Eb":303,"50%":304,"":305}' AS JSON) AS VARIANT)),
        (4,CAST(CAST('{"a.b":401,"a":{"b":402},"a%2Eb":403,"50%":404,"":405}' AS JSON) AS VARIANT),NULL,NULL)"""
    qt_base """SELECT k,coalesce(v1,v2,legacy),
        coalesce(v1,v2,legacy)['a.b'],coalesce(v1,v2,legacy)['a']['b'],
        coalesce(v1,v2,legacy)['a%2Eb'],coalesce(v1,v2,legacy)['50%'],coalesce(v1,v2,legacy)['']
        FROM dotted_key_escape_common_type INDEX dotted_key_escape_common_type ORDER BY k"""
    qt_physical_mv """SELECT kk,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
        FROM dotted_key_escape_common_type INDEX dotted_escape_common_mv ORDER BY kk"""

    sql "DROP TABLE IF EXISTS dotted_key_escape_common_ctas"
    sql """CREATE TABLE dotted_key_escape_common_ctas DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS
        SELECT k,coalesce(v1,v2,legacy) AS v FROM dotted_key_escape_common_type"""
    qt_ctas_schema "DESC dotted_key_escape_common_ctas"
    qt_ctas """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
        FROM dotted_key_escape_common_ctas ORDER BY k"""
    // A later write uses the derived target schema as well.
    sql """INSERT INTO dotted_key_escape_common_ctas
        SELECT k+10,coalesce(v1,v2,legacy) FROM dotted_key_escape_common_type WHERE k=4"""
    qt_ctas_insert """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
        FROM dotted_key_escape_common_ctas WHERE k=14 ORDER BY k"""

    sql "DROP TABLE IF EXISTS dotted_key_escape_common_case"
    sql """CREATE TABLE dotted_key_escape_common_case DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS
        SELECT k,CASE WHEN k IN (1,4) THEN v1 WHEN k=2 THEN v2 ELSE legacy END AS v
        FROM dotted_key_escape_common_type"""
    qt_case """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
        FROM dotted_key_escape_common_case ORDER BY k"""

    sql "DROP TABLE IF EXISTS dotted_key_escape_common_union"
    sql """CREATE TABLE dotted_key_escape_common_union DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS
        SELECT k,v1 AS v FROM dotted_key_escape_common_type WHERE k IN (1,4)
        UNION ALL SELECT k,v2 FROM dotted_key_escape_common_type WHERE k=2
        UNION ALL SELECT k,legacy FROM dotted_key_escape_common_type WHERE k=3"""
    qt_union """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
        FROM dotted_key_escape_common_union ORDER BY k"""
}
