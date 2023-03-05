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

suite ("create_mv_datatype") {
    def wait_for_create_mv_finish = { table_name, OpTimeout ->
        def delta_time = 1000
        for(useTime = 0; useTime <= OpTimeout; useTime += delta_time){
            alter_res = sql """SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                 break
            }
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_create_mv_finish timeout")
    }

    sql "ADMIN SET FRONTEND CONFIG ('enable_struct_type' = 'true')"
    sql "ADMIN SET FRONTEND CONFIG ('enable_map_type' = 'true')"

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

    def success = false

    // 1. special column The first column could not be - dup key mv prefix
    success = false
    try {
        sql """create materialized view mv as select c_jsonb, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("The first column could not be"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_array, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("The first column could not be"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_map, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("The first column could not be"), e.getMessage())
    }
    assertFalse(success)

    success = false
    try {
        sql """create materialized view mv as select c_struct, c_int from base_table;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("The first column could not be"), e.getMessage())
    }
    assertFalse(success)


    // 2. special column The first column could not be mv agg key
    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_jsonb, count(c_bigint) from base_table group by c_bigint, c_int, c_jsonb;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("can not be key"), e.getMessage())
    }
    assertFalse(success)


    // 3. special column can not be in ORDER BY
    success = false
    try {
        sql """create materialized view mv as select c_bigint, c_int, c_jsonb from base_table order by c_bigint, c_int, c_jsonb;"""
        success = true
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("can not be key"), e.getMessage())
    }
    assertFalse(success)


    // 4. special column early stop key column list

    // create mv success, key is c_bigint, c_int, c_float
    sql """create materialized view mv1 as select c_bigint, c_int, c_float from base_table;"""
    wait_for_create_mv_finish('base_table', 60000)
    qt_select_desc1 """desc base_table all;"""

    // create mv success, key is c_bigint, c_int
    sql """create materialized view mv2 as select c_bigint, c_int, c_jsonb from base_table;"""
    wait_for_create_mv_finish('base_table', 60000)
    qt_select_desc2 """desc base_table all;"""

    // // create mv success, key is c_bigint, c_int
    // sql """create materialized view mv3 as select c_bigint, c_int, c_array from base_table;"""
    // wait_for_create_mv_finish('base_table', 60000)
    // qt_select_desc3 """desc base_table all;"""

    // // create mv success, key is c_bigint, c_int
    // sql """create materialized view mv4 as select c_bigint, c_int, c_map from base_table;"""
    // wait_for_create_mv_finish('base_table', 60000)
    // qt_select_desc4 """desc base_table all;"""

    // // create mv success, key is c_bigint, c_int
    // sql """create materialized view mv5 as select c_bigint, c_int, c_struct from base_table;"""
    // wait_for_create_mv_finish('base_table', 60000)
    // qt_select_desc5 """desc base_table all;"""    

}
