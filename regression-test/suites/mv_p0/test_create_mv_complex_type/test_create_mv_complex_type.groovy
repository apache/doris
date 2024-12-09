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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("create_mv_complex_type") {

    sql """ DROP TABLE IF EXISTS base_table; """
    sql """
            create table base_table (
                c_int INT,
                c_bigint BIGINT(10),
                c_float BIGINT,
                c_jsonb JSONB,
                c_array ARRAY<INT>,
                c_map MAP<STRING, INT>,
                c_struct STRUCT<a:INT, b:INT>
            )
            duplicate key (c_int)
            distributed BY hash(c_int) buckets 3
            properties("replication_num" = "1");
        """

    sql """insert into base_table select 1, 100000, 1.0, '{"jsonk1": 123}', [100, 200], {"k1": 10}, {1, 2};"""

    sql """alter table base_table modify column c_int set stats ('row_count'='1');"""

    def success = false

    // 1. special column - mv dup key
    success = false
    try {
        sql """create materialized view mv as select c_jsonb, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_jsonb from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_array, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_array from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_map, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_map from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_struct, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_struct from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("not support to create materialized view"), e.getMessage())
    }
    assertFalse(success)


    // 2. special column - mv agg key
    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_jsonb, count(c_bigint) from base_table group by c_bigint, c_int, c_jsonb;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_array, count(c_bigint) from base_table group by c_bigint, c_int, c_array;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_map, count(c_bigint) from base_table group by c_bigint, c_int, c_map;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_struct, count(c_bigint) from base_table group by c_bigint, c_int, c_struct;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)


    // 3. special column - ORDER BY
    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_jsonb from base_table order by c_bigint, c_int, c_jsonb;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_array from base_table order by c_bigint, c_int, c_array;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_map from base_table order by c_bigint, c_int, c_map;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_struct from base_table order by c_bigint, c_int, c_struct;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("don't support filter, group by"), e.getMessage())
    }
    assertFalse(success)
}
