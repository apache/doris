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

suite("create_nestedtypes_with_schemachange", "p0") {

    def create_nested_table_and_schema_change = {testTablex, nested_type, column_name, error ->
        // create basic type
        sql "DROP TABLE IF EXISTS $testTablex"
        sql """ CREATE TABLE $testTablex (
                     col0 BIGINT NOT NULL,  col2 int NOT NULL, col3 array<int> NULL, col4 map<int, int> NULL, col5 struct<f1: int> NULL
                )
                /* mow */
                UNIQUE KEY(col0) DISTRIBUTED BY HASH(col0) BUCKETS 4 PROPERTIES (
                  "enable_unique_key_merge_on_write" = "true",
                  "replication_num" = "1"
                ); """
        // alter table add nested type
        if (error != '') {
            // check nested type do not support other type
            test {
               sql "ALTER TABLE $testTablex MODIFY COLUMN $column_name $nested_type AFTER col0"
               exception (error)
            }
        } else {
            // check nested type can only support change order
            sql "ALTER TABLE $testTablex ADD COLUMN $column_name $nested_type"
            sql "ALTER TABLE $testTablex MODIFY COLUMN $column_name $nested_type AFTER col0"
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='$testTablex' ORDER BY createtime DESC LIMIT 1 """
                time 600
            }
        }
        // desc table
        qt_sql "DESC $testTablex"
    }

    // array
    create_nested_table_and_schema_change.call("test_array_schemachange", "ARRAY<STRING>", "col_array", '')
    // map
    create_nested_table_and_schema_change.call("test_map_schemachange", "MAP<char(32), string>", "col_map", '')
    // struct
    create_nested_table_and_schema_change.call("test_struct_schemachange", "STRUCT<f1: varchar(1)>", "col_struct", '')

    // array with other type
    create_nested_table_and_schema_change.call("test_array_schemachange_1", "ARRAY<STRING>", "col3", "errCode = 2");
    // map with other type
    create_nested_table_and_schema_change.call("test_map_schemachange_1", "MAP<char(32), string>", "col4", "errCode = 2");
    // struct with other type
    create_nested_table_and_schema_change.call("test_struct_schemachange_1", "STRUCT<f1: varchar(1)>", "col5", "errCode = 2");

    def create_nested_table_and_schema_change_null_trans = {testTablex, nested_type, column_name, notNull2Null ->
        def null_define = "NULL"
        if (notNull2Null) {
            null_define = "NOT NULL"
        }
        // create basic type
        sql "DROP TABLE IF EXISTS $testTablex"
        sql """ CREATE TABLE IF NOT EXISTS $testTablex (
                     col0 BIGINT NOT NULL,
                     col2 int NOT NULL,
                     col3 array<int> $null_define,
                     col4 map<int, int> $null_define,
                     col5 struct<f1: int> $null_define,
                     col6 variant $null_define
                )
                /* mow */
                UNIQUE KEY(col0) DISTRIBUTED BY HASH(col0) BUCKETS 4 PROPERTIES (
                  "enable_unique_key_merge_on_write" = "true",
                  "replication_num" = "1",
                  'light_schema_change' = 'true', 'disable_auto_compaction'='true'
                ); """
        // insert data
        sql """ INSERT INTO $testTablex VALUES (1, 2, array(1, 2), map(1, 2), named_struct('f1', 1), '{"a": [1,2,3]}')"""
        // select
        qt_sql_before "select * from $testTablex"

        if (notNull2Null) {
            sql "ALTER TABLE $testTablex MODIFY COLUMN $column_name $nested_type NULL"
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='$testTablex' ORDER BY createtime DESC LIMIT 1 """
                time 600
            }
        } else {
            // schema change from null to non-nullable is not supported
            test {
               sql "ALTER TABLE $testTablex MODIFY COLUMN $column_name $nested_type NOT NULL"
               exception "Can not change from nullable to non-nullable"
            }
        }
        // desc table
        qt_sql "DESC $testTablex"
        qt_sql_after "select * from $testTablex"
    }

    // array
    create_nested_table_and_schema_change_null_trans.call("test_array_schemachange_null", "ARRAY<INT>", "col3", false)
    create_nested_table_and_schema_change_null_trans.call("test_array_schemachange_null1", "ARRAY<INT>", "col3", true)
    // map
    create_nested_table_and_schema_change_null_trans.call("test_map_schemachange_null", "Map<INT, INT>", "col4", false)
    create_nested_table_and_schema_change_null_trans.call("test_map_schemachange_null1", "Map<INT, INT>", "col4", true)
    // struct
    create_nested_table_and_schema_change_null_trans.call("test_struct_schemachange_null", "struct<f1: int>", "col5", false)
    create_nested_table_and_schema_change_null_trans.call("test_struct_schemachange_null1", "struct<f1: int>", "col5", true)
    // variant
    // create_nested_table_and_schema_change_null_trans.call("test_v_schemachange_null", "variant", "col6", false)
    // create_nested_table_and_schema_change_null_trans.call("test_v_schemachange_null1", "variant", "col6", true)

}
