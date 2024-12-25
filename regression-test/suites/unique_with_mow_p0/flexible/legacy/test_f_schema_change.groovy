
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
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_f_schema_change", "p0") {

    for (def row_store : [false, true]) {
        def tableName = "test_f_schema_change"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
                CREATE TABLE ${tableName} (
                    `c0` int NULL,
                    `c1` int NULL,
                    `c2` int NULL,
                    `c3` int NULL,
                    `c4` int NULL,
                    `c5` int NULL,
                    `c6` int NULL,
                    `c7` int NULL,
                    `c8` int NULL,
                    `c9` int NULL)
                    UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                    PROPERTIES(
                        "replication_num" = "1",
                        "disable_auto_compaction" = "true",
                        "light_schema_change" = "true",
                        "enable_unique_key_merge_on_write" = "true",
                        "enable_unique_key_skip_bitmap_column" = "true",
                        "store_row_column" = "${row_store}")
        """

        sql """insert into ${tableName} select number,number,number,number,number,number,number,number,number,number from numbers("number"="15"); """
        order_qt_sql " select * from ${tableName}  "

        def doSchemaChange = { cmd ->
            sql cmd
            waitForSchemaChangeDone {
                sql """SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
                time 20000
            }
        }

        // 1. add value column
        doSchemaChange " ALTER table ${tableName} add column c10 INT DEFAULT '999' "
        // 1.1 load data without new column
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load_without_new_column.json'
            time 10000 // limit inflight 10s
        }
        // check data, new column is filled by default value.
        order_qt_add_value_col_1 " select * from ${tableName}  "
        // 1.2 load data with new column
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load_with_new_column.json'
            time 10000 // limit inflight 10s
        }
        // check data, new column is filled by given value.
        order_qt_add_value_col_2 " select * from ${tableName}  "


        // 2. test drop value column
        doSchemaChange " ALTER table ${tableName} DROP COLUMN c8 "
        // 2.1 test load data without delete column
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load3.json'
            time 10000 // limit inflight 10s

        }
        sql "sync"
        order_qt_drop_value_col_1 " select * from ${tableName}  "
        // 2.2 test load data with delete column, stream load will ignore the
        // non-existing column
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load4.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_drop_value_col_2 " select * from ${tableName}  "


        // 3. test update value column
        doSchemaChange " ALTER table ${tableName} MODIFY COLUMN c2 double "
        // 3.1 test load data with update column
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load5.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_update_value_col " select * from ${tableName}  "


        // 4. test add key column
        doSchemaChange " ALTER table ${tableName} ADD COLUMN add_k1 int key null"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load6.json'
            time 10000 // limit inflight 10s
        }
        order_qt_add_key_col " select * from ${tableName}  "

        // 5. reorder columns
        doSchemaChange " ALTER table ${tableName} order by (add_k1,c0,c2,c1,c3,c4,c10,c6,c5,c9,c7);"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load7.json'
            time 10000 // limit inflight 10s
        }
        order_qt_reorder_cols " select * from ${tableName}  "

        // 6. create inverted index
        doSchemaChange " CREATE INDEX test ON ${tableName} (c1) USING inverted "
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load8.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_add_inverted_index " select * from ${tableName}  "

        // 7. test change properties
        doSchemaChange " ALTER TABLE ${tableName} set ('disable_auto_compaction' = 'false') "
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load9.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_change_properties " select * from ${tableName}  "

        // 8. rename value column
        // 8.1 rename column
        doSchemaChange "alter table ${tableName} rename column c5 rename_c5"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load10.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_rename_value_col_1 " select * from ${tableName}  "
        // 8.2 rename back
        doSchemaChange "alter table ${tableName} rename column rename_c5 c5"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load11.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_rename_value_col_2 " select * from ${tableName}  "

        // 9. rename key column
        // 9.1 rename column
        doSchemaChange "alter table ${tableName} rename column add_k1 k1"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load12.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_rename_key_col_1 " select * from ${tableName}"
        // 9.2 rename back
        doSchemaChange "alter table ${tableName} rename column k1 add_k1"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'schema_change/load13.json'
            time 10000 // limit inflight 10s
        }
        sql "sync"
        order_qt_rename_key_col_2 " select * from ${tableName}  "

        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
