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

suite("nereids_scalar_fn_StructNullsafe", "p0") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // table nullsafe array
    sql """DROP TABLE IF EXISTS fn_test_nullsafe_struct"""
    sql """
        CREATE TABLE IF NOT EXISTS `fn_test_nullsafe_struct` (
            `id` int not null,
            `struct` struct<id: int, name: string>
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        properties("replication_num" = "1")
    """
    
    // insert into fn_test_nullsafe_array with null element
    sql """
        INSERT INTO fn_test_nullsafe_struct VALUES 
        (1, STRUCT(1, 'Alice')),
        (2, STRUCT(null, 'Bob')),
        (3, STRUCT(3, null)),
        (4, NULL),
        (5, STRUCT(null, null))
    """

    // test function struct 
    qt_sql_struct_element_by_index "SELECT struct_element(struct, 1), struct_element(struct, 2) FROM fn_test_nullsafe_struct ORDER BY id"
    qt_sql_struct_element_by_name "SELECT struct_element(struct, 'id'), struct_element(struct, 'name') FROM fn_test_nullsafe_struct ORDER BY id"

    // test function named_struct
    qt_sql_literal_named_struct "SELECT named_struct('id', 1, 'name', NULL)"
    qt_sql_literal_struct_element "SELECT struct_element(named_struct('id', NULL, 'name', 'Tom'), 'id'), struct_element(named_struct('id', 2, 'name', NULL), 'name')"

    // test function struct_element 
    qt_sql_literal_struct "SELECT STRUCT(NULL, 'X')"
}