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


import com.google.common.collect.Lists
import org.apache.commons.lang3.StringUtils
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_nested_types_insert_into_with_literal", "p0") {
    sql 'use regression_test_datatype_p0_nested_types'
    // old planner does not support cast empty
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    def table_names = [
            "two_level_array_array_a",
            "two_level_array_map_a",
            "two_level_array_struct_a",

            "two_level_map_array_a",
            "two_level_map_map_a",
            "two_level_map_struct_a",

            "two_level_struct_array_a",
            "two_level_struct_map_a",
            "two_level_struct_struct_a"
    ]

    // notice : we do not suggest to use this literal {} to present empty struct, please use struct() instead
    def null_literals = ["[[]]", "[{}]", "array(struct(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null))",
                        "{null:[]}", "{null:{}}", "{}",
                        "struct([])", "struct({})", "struct(struct(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null))"]

    def colNameArr = ["c_bool", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_largeint", "c_float",
                      "c_double", "c_decimal", "c_decimalv3", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string"]

    // create tables
    // (0,0) (0,1) (0,2) (1,0) (1,1) (1,2) (2,0) (2,1) (2,2)
    for (int i = 0; i < 3; ++i) {
        for (int j = 0; j < 3; ++j) {
            sql """ DROP TABLE IF EXISTS ${table_names[i*3+j]} """
            String result = create_table_with_nested_type(2, [i, j], table_names[i*3+j])
            sql result
        }
    }

    // insert into empty literal
    for (int i = 0; i < 9; ++i) {
        String insertSql = "INSERT INTO ${table_names[i]} VALUES(1, "
        for (int j = 0; j < colNameArr.size(); ++j) {
            insertSql += "${null_literals[i]},"
        }
        insertSql = insertSql.substring(0, insertSql.length() - 1) + ")"
        sql insertSql
        qt_sql """ select * from ${table_names[i]} order by k1 """
    }
}
