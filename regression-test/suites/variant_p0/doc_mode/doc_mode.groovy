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

suite("regression_test_variant_doc_mode", "p0"){
    sql """ set default_variant_enable_doc_mode = true """

  
    def table_min0 = "doc_mode_min0"
    sql """ DROP TABLE IF EXISTS ${table_min0} """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_min0} (
            k bigint,
            v variant<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="0")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    def show_create_min0 = sql """ show create table ${table_min0} """
    logger.info("show_create_min0: " + show_create_min0.toString())
    assertTrue(show_create_min0.toString().contains("variant_enable_doc_mode"))
    assertTrue(show_create_min0.toString().contains("variant_doc_materialization_min_rows"))
    assertTrue(show_create_min0.toString().contains("\"0\"") || show_create_min0.toString().contains("= 0"))

    // multiple inserts to generate multiple rowsets
    sql """insert into ${table_min0} values
        (1, '{"a":1,"b":{"c":"x"},"arr":[1,2]}'),
        (2, '{"a":2,"b":{"c":"y"},"arr":[3]}'),
        (3, '{"a":3,"b":{"c":"z"},"arr":[]}');"""
    sql """insert into ${table_min0} values
        (4, '{"a":4,"b":{"c":"x2"},"arr":[4,5,6]}'),
        (5, '{"a":5,"b":{"c":"y2"},"arr":[7]}');"""
    sql """insert into ${table_min0} values
        (6, '{"a":6,"b":{"c":"z2"},"arr":[8,9]}');"""

    // runtime proof that doc snapshot column exists in variant map
    def vtype_min0 = sql """select variant_type(v) from ${table_min0} where k = 1"""
    assertTrue(vtype_min0.toString().contains("__DORIS_VARIANT_DOC_VALUE__"))

    def before_min0 = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_min0}
        order by k;
    """
    trigger_and_wait_compaction(table_min0, "cumulative")
    def after_min0 = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_min0}
        order by k;
    """
    assertEquals(before_min0, after_min0)
    logger.info("after_min0: " + after_min0.toString())

   
    def table_ins = "doc_mode_insert_into"
    sql """ DROP TABLE IF EXISTS ${table_ins} """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_ins} (
            k bigint,
            v variant<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="0")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """insert into ${table_ins} select * from ${table_min0}"""
    def vtype_ins = sql """select variant_type(v) from ${table_ins} where k = 1"""
    assertTrue(vtype_ins.toString().contains("__DORIS_VARIANT_DOC_VALUE__"))

    def before_ins = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_ins}
        order by k;
    """
    trigger_and_wait_compaction(table_ins, "cumulative")
    def after_ins = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_ins}
        order by k;
    """
    assertEquals(before_ins, after_ins)
    assertEquals(before_min0, before_ins)

    // ----------------------------
    // Case C: create table like
    // Covers: create table like, insert into ... select, compaction consistency
    // ----------------------------
    def table_like = "doc_mode_like"
    sql """ DROP TABLE IF EXISTS ${table_like} """
    sql """ create table ${table_like} like ${table_min0} """
    def show_create_like = sql """ show create table ${table_like} """
    assertTrue(show_create_like.toString().contains("variant_enable_doc_mode"))
    assertTrue(show_create_like.toString().contains("variant_doc_materialization_min_rows"))
    assertTrue(show_create_like.toString().contains("\"0\"") || show_create_like.toString().contains("= 0"))
    logger.info("show_create_like: " + show_create_like.toString())

    sql """insert into ${table_like} select * from ${table_min0}"""
    def vtype_like = sql """select variant_type(v) from ${table_like} where k = 1"""
    assertTrue(vtype_like.toString().contains("__DORIS_VARIANT_DOC_VALUE__"))

    def before_like = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_like}
        order by k;
    """
    trigger_and_wait_compaction(table_like, "cumulative")
    def after_like = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_like}
        order by k;
    """
    assertEquals(before_like, after_like)
    assertEquals(before_min0, before_like)

    // ----------------------------
    // Case D: doc_mode + min_rows very large
    // Covers: different variant_doc_materialization_min_rows behavior, compaction consistency
    // ----------------------------
    def table_min_big = "doc_mode_min_big"
    sql """ DROP TABLE IF EXISTS ${table_min_big} """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_min_big} (
            k bigint,
            v variant<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="10000000")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    def show_create_min_big = sql """ show create table ${table_min_big} """
    assertTrue(show_create_min_big.toString().contains("variant_enable_doc_mode"))
    assertTrue(show_create_min_big.toString().contains("variant_doc_materialization_min_rows"))
    assertTrue(show_create_min_big.toString().contains("10000000"))

    sql """insert into ${table_min_big} values
        (1, '{"a":1,"b":{"c":"x"},"arr":[1,2]}'),
        (2, '{"a":2,"b":{"c":"y"},"arr":[3]}'),
        (3, '{"a":3,"b":{"c":"z"},"arr":[]}');"""
    sql """insert into ${table_min_big} values
        (4, '{"a":4,"b":{"c":"x2"},"arr":[4,5,6]}'),
        (5, '{"a":5,"b":{"c":"y2"},"arr":[7]}');"""
    sql """insert into ${table_min_big} values
        (6, '{"a":6,"b":{"c":"z2"},"arr":[8,9]}');"""

    def vtype_min_big = sql """select variant_type(v) from ${table_min_big} where k = 1"""
    assertTrue(vtype_min_big.toString().contains("__DORIS_VARIANT_DOC_VALUE__"))

    def before_min_big = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_min_big}
        order by k;
    """
    trigger_and_wait_compaction(table_min_big, "cumulative")
    def after_min_big = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_min_big}
        order by k;
    """
    assertEquals(before_min_big, after_min_big)
    // correctness should be identical regardless of min_rows
    assertEquals(before_min0, before_min_big)

    // ----------------------------
    // Case E: session default min_rows != column properties (override)
    // Covers: column properties override session variable
    // ----------------------------
    sql """ set default_variant_doc_materialization_min_rows = 10000000 """
    def table_override = "doc_mode_override_min0"
    sql """ DROP TABLE IF EXISTS ${table_override} """
    // column explicitly sets min_rows=0, should override session default
    sql """
        CREATE TABLE IF NOT EXISTS ${table_override} (
            k bigint,
            v variant<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="0")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    def show_create_override = sql """ show create table ${table_override} """
    assertTrue(show_create_override.toString().contains("variant_doc_materialization_min_rows"))
    assertTrue(show_create_override.toString().contains("\"0\"") || show_create_override.toString().contains("= 0"))

    sql """insert into ${table_override} values
        (1, '{"a":1,"b":{"c":"x"},"arr":[1,2]}'),
        (2, '{"a":2,"b":{"c":"y"},"arr":[3]}'),
        (3, '{"a":3,"b":{"c":"z"},"arr":[]}');"""
    sql """insert into ${table_override} values
        (4, '{"a":4,"b":{"c":"x2"},"arr":[4,5,6]}'),
        (5, '{"a":5,"b":{"c":"y2"},"arr":[7]}');"""
    sql """insert into ${table_override} values
        (6, '{"a":6,"b":{"c":"z2"},"arr":[8,9]}');"""
    def vtype_override = sql """select variant_type(v) from ${table_override} where k = 1"""
    assertTrue(vtype_override.toString().contains("__DORIS_VARIANT_DOC_VALUE__"))

    def before_override = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_override}
        order by k;
    """
    trigger_and_wait_compaction(table_override, "cumulative")
    def after_override = sql_return_maparray """
        select
            k,
            cast(v as string) as v_str,
            cast(v['a'] as int) as a_int,
            cast(v['b']['c'] as string) as bc_str,
            size(cast(v['arr'] as array<int>)) as arr_sz
        from ${table_override}
        order by k;
    """
    assertEquals(before_override, after_override)
    assertEquals(before_min0, before_override)

    // best-effort reset
    sql """ set default_variant_doc_materialization_min_rows = 0 """
}