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

    def create_nested_table_and_schema_change = {testTablex, nested_type, column_name ->
        // create basic type
        sql "DROP TABLE IF EXISTS $testTablex"
        sql """ CREATE TABLE $testTablex (
                     col0 BIGINT NOT NULL,  col2 int NOT NULL
                )
                /* mow */
                UNIQUE KEY(col0) DISTRIBUTED BY HASH(col0) BUCKETS 4 PROPERTIES (
                  "enable_unique_key_merge_on_write" = "true",
                  "replication_num" = "1"
                ); """
        // alter table add nested type
        sql "ALTER TABLE $testTablex ADD COLUMN $column_name $nested_type"
        // alter table modify column order
        sql "ALTER TABLE $testTablex MODIFY COLUMN $column_name $nested_type AFTER col0"
        // desc table
        qt_sql "DESC $testTablex"
    }

    // array
    create_nested_table_and_schema_change.call("test_array_schemachange", "ARRAY<STRING>", "col_array")
    // map
    create_nested_table_and_schema_change.call("test_map_schemachange", "MAP<char(32), string>", "col_map")
    // struct
    create_nested_table_and_schema_change.call("test_struct_schemachange", "STRUCT<f1: varchar(1)>", "col_struct")

}
