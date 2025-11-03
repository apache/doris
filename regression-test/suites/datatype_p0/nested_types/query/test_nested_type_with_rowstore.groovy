import org.apache.commons.lang3.StringUtils

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

suite("test_nested_type_with_rowstore") {
    sql """ DROP TABLE IF EXISTS ct_table;"""
    sql """CREATE TABLE ct_table ( `id` int(11) NOT NULL COMMENT "用户 ID", `c_varchar` varchar(65533) NULL COMMENT "用户姓名", `c_jsonb` JSONB NULL, `c_array` ARRAY<INT> NULL, `c_map` MAP<STRING, INT> NULL, `c_struct` STRUCT<a:INT, b:INT> NULL) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true", "store_row_column" = "true");"""

    sql """ insert into ct_table values(2, "doris2", '{"jsonk3": 333, "jsonk4": 444}', [300, 400], {"k2": 20}, {3, 4});"""
    sql """ insert into ct_table values(1, "doris1", '{"jsonk1": 123, "jsonk2": 456}', [100, 200], {"k1": 10}, {1, 2});"""

    qt_sql """ select * from ct_table order by id;"""
    // point sql
    qt_sql """ select * from ct_table where id = 1"""

    // column refresh
    streamLoad {
            table "ct_table"
            time 10000
            set 'partial_columns', 'true'
            set 'strict_mode', 'false'
            set 'columns', 'id,c_varchar'
            file 'varchar.tsv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
    }

    sql "sync"

    // select and check
    qt_sql """ select * from ct_table order by id;"""
    // point sql
    qt_sql """ select * from ct_table where id = 1"""
}
