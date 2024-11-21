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

suite('test_flexible_partial_update_upgrade', 'p0,restart_fe') {

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")
        def tableName = "test_f_upgrade_${use_row_store}"
        sql """alter table ${tableName} enable feature "UPDATE_FLEXIBLE_COLUMNS"; """
        show_res = sql "show create table ${tableName}"
        assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))
        qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test1.json"
            time 20000
        }
        qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"

        sql "set enable_unique_key_partial_update=true;"
        sql "set enable_insert_strict=false;"
        sql "sync;"
        sql "insert into ${tableName}(k,v1,v3,v5) values(1,999,999,999),(2,888,888,888),(5,777,777,777),(20,555,555,555);"
        sql "set enable_unique_key_partial_update=false;"
        sql "set enable_insert_strict=true;"
        sql "sync;"
        qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"

        sql "delete from ${tableName} where k>=3 and k<=6;"
        qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"

        sql "insert into ${tableName} values(3,10,10,10,10,10),(30,11,11,11,11,11),(4,12,12,12,12,12);"
        qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"
    }
}