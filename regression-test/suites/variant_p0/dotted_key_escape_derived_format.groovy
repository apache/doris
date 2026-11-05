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

suite("dotted_key_escape_derived_format", "p0") {
    def oldDerived = sql("show variables like 'enable_variant_escape_dots_in_keys_for_derived_columns'")[0][1]
    try {
        sql "set group_commit='off_mode'"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=false"
        sql "set enable_variant_escape_dots_in_keys_for_derived_columns=false"
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_source"
        sql """CREATE TABLE dotted_key_escape_derived_source(k INT,
            legacy VARIANT<PROPERTIES("variant_escape_dots_in_keys"="false", "variant_enable_nested_group"="false")>,
            v VARIANT<PROPERTIES("variant_escape_dots_in_keys"="true", "variant_enable_nested_group"="false",
                "variant_max_subcolumns_count"="1")>)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO dotted_key_escape_derived_source VALUES
            (0,parse_to_variant('{"a":{"b":11}}'),NULL),
            (1,NULL,parse_to_variant('{"a.b":101,"a":{"b":102},"a%2Eb":103,"50%":104,"":105}')),
            (2,NULL,parse_to_variant('{"a.b":201,"a%2Eb":203,"50%":204,"":205}'))"""

        // The recursive result has the anchor's legacy compute type. Its values remain canonical;
        // creating a storage column must also preserve values contributed by the escaped branch.
        def recursive = """WITH RECURSIVE r(k,v) AS (
            SELECT k,legacy FROM dotted_key_escape_derived_source WHERE k=0
            UNION ALL
            SELECT s.k,s.v FROM r JOIN dotted_key_escape_derived_source s ON r.k=0 AND s.k>0)
            SELECT k,v FROM r"""
        qt_recursive_select """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
            FROM (${recursive}) result ORDER BY k"""
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_recursive"
        sql """CREATE TABLE dotted_key_escape_derived_recursive DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1") AS ${recursive}"""
        qt_recursive_schema "DESC dotted_key_escape_derived_recursive"
        qt_recursive_ctas """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
            FROM dotted_key_escape_derived_recursive ORDER BY k"""
        qt_reverse_recursive """WITH RECURSIVE r(k,v) AS (
            SELECT k,v FROM dotted_key_escape_derived_source WHERE k=1
            UNION ALL
            SELECT s.k,s.legacy FROM r JOIN dotted_key_escape_derived_source s ON r.k=1 AND s.k=0)
            SELECT k,v,v['a.b'],v['a']['b'] FROM r ORDER BY k"""

        // A scalar subquery must not hide the source format from CTAS.
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_subquery"
        sql """CREATE TABLE dotted_key_escape_derived_subquery DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1") AS
            SELECT CAST(1 AS INT) AS k,(SELECT v FROM dotted_key_escape_derived_source WHERE k=1) AS v"""
        qt_subquery """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],v['']
            FROM dotted_key_escape_derived_subquery ORDER BY k"""

        // A table with no physical partitions still declares a storage format. The empty CTAS must
        // retain that choice for later writes, even after partition pruning removes its source scan.
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_empty_source"
        sql """CREATE TABLE dotted_key_escape_derived_empty_source(k INT,dt DATE,
            v VARIANT<PROPERTIES("variant_escape_dots_in_keys"="true", "variant_enable_nested_group"="false")>)
            DUPLICATE KEY(k,dt) PARTITION BY RANGE(dt) () DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1")"""
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_empty"
        sql """CREATE TABLE dotted_key_escape_derived_empty DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1") AS SELECT k,coalesce(v,v) AS v
            FROM dotted_key_escape_derived_empty_source WHERE false"""
        sql """INSERT INTO dotted_key_escape_derived_empty VALUES
            (1,parse_to_variant('{"a.b":501,"a":{"b":502},"a%2Eb":503}'))"""
        qt_empty_source_later_write """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb']
            FROM dotted_key_escape_derived_empty ORDER BY k"""

        sql "set enable_variant_escape_dots_in_keys_for_derived_columns=true"
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_literal"
        sql """CREATE TABLE dotted_key_escape_derived_literal DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1") AS SELECT CAST(1 AS INT) AS k,
            parse_to_variant('{"a.b":301,"a":{"b":302},"a%2Eb":303}') AS v"""
        qt_default_schema "DESC dotted_key_escape_derived_literal"
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_bare"
        sql """CREATE TABLE dotted_key_escape_derived_bare DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1") AS SELECT k,legacy AS v
            FROM dotted_key_escape_derived_source WHERE k=0"""
        qt_bare_legacy_schema "DESC dotted_key_escape_derived_bare"
        qt_bare_legacy "SELECT k,v FROM dotted_key_escape_derived_bare ORDER BY k"
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_like"
        sql "CREATE TABLE dotted_key_escape_derived_like LIKE dotted_key_escape_derived_bare"
        qt_bare_like_schema "DESC dotted_key_escape_derived_like"
        sql "DROP TABLE IF EXISTS dotted_key_escape_derived_explicit"
        sql """CREATE TABLE dotted_key_escape_derived_explicit(k INT,v VARIANT,v2 VARIANT)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1")"""
        qt_explicit_ddl_schema "DESC dotted_key_escape_derived_explicit"
        sql """INSERT INTO dotted_key_escape_derived_explicit VALUES
            (1,parse_to_variant('{"a":{"b":611}}'),NULL)"""
        def db = context.config.getDbNameByFile(context.file)
        create_sync_mv(db, 'dotted_key_escape_derived_explicit', 'dotted_escape_derived_default_mv',
                "SELECT k AS kk,coalesce(v,v2) AS mv_v FROM dotted_key_escape_derived_explicit")
        sql "set enable_variant_escape_dots_in_keys_for_derived_columns=false"
        sql """INSERT INTO dotted_key_escape_derived_explicit VALUES
            (2,NULL,parse_to_variant('{"a":{"b":621}}'))"""
        // The new MV uses escaped paths even though its base columns remain legacy. Both historical
        // backfill and later writes preserve canonical objects, without changing their expression types.
        qt_default_mv """SELECT kk,mv_v,mv_v['a.b'],mv_v['a']['b']
            FROM dotted_key_escape_derived_explicit INDEX dotted_escape_derived_default_mv ORDER BY kk"""
        // Changing a creation default does not change a previously created column's writer.
        sql """INSERT INTO dotted_key_escape_derived_literal VALUES
            (2,parse_to_variant('{"a.b":401,"a":{"b":402},"a%2Eb":403}'))"""
        qt_default_disabled_write """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb']
            FROM dotted_key_escape_derived_literal ORDER BY k"""
    } finally {
        sql "set enable_variant_escape_dots_in_keys_for_derived_columns=${oldDerived}"
    }
}
