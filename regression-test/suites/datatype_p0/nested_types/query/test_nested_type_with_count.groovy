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

suite("test_nested_type_with_count") {
    def table_names = ["test_array_one_level", "test_map_one_level", "test_struct_one_level"]

    def colNameArr = ["c_bool", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_largeint", "c_float",
                      "c_double", "c_decimal", "c_decimalv3", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string"]

    // create tables
    for (int i = 0; i < table_names.size(); ++i) {
        sql """ DROP TABLE IF EXISTS ${table_names[i]} """
        String result = create_table_with_nested_type(1, [i], table_names[i])
        sql result
        for (int j = 0; j < colNameArr.size(); ++j) {
            qt_sql_one_level "select count(${colNameArr[j]}) from ${table_names[i]}"
        }
    }

    // two-level
    def table_names_level2 = [
                                        "two_level_array_array",
                                        "two_level_array_map",
                                        "two_level_array_struct",

                                        "two_level_map_array",
                                        "two_level_map_map",
                                        "two_level_map_struct",

                                        "two_level_struct_array",
                                        "two_level_struct_map",
                                        "two_level_struct_struct"
    ]
    // create tables
    // (0,0) (0,1) (0,2) (1,0) (1,1) (1,2) (2,0) (2,1) (2,2)
    for (int i = 0; i < 3; ++i) {
        for (int j = 0; j < 3; ++j) {
            sql """ DROP TABLE IF EXISTS ${table_names_level2[i*3+j]} """
            String result = create_table_with_nested_type(2, [i, j], table_names_level2[i*3+j])
            sql result
            for (int c = 0; c < colNameArr.size(); ++c) {
                qt_sql_two_level "select count(${colNameArr[c]}) from ${table_names_level2[i*3+j]}"
            }
        }
    }

    // three-level
    def table_names_level3 = [
            "three_level_array_array_array",
            "three_level_array_array_map",
            "three_level_array_array_struct",
            "three_level_array_map_array",
            "three_level_array_map_map",
            "three_level_array_map_struct",
            "three_level_array_struct_array",
            "three_level_array_struct_map",
            "three_level_array_struct_struct",

            "three_level_map_array_array",
            "three_level_map_array_map",
            "three_level_map_array_struct",
            "three_level_map_map_array",
            "three_level_map_map_map",
            "three_level_map_map_struct",
            "three_level_map_struct_array",
            "three_level_map_struct_map",
            "three_level_map_struct_struct",

            "three_level_struct_array_array",
            "three_level_struct_array_map",
            "three_level_struct_array_struct",
            "three_level_struct_map_array",
            "three_level_struct_map_map",
            "three_level_struct_map_struct",
            "three_level_struct_struct_array",
            "three_level_struct_struct_map",
            "three_level_struct_struct_struct"
    ]

    // create tables
    // array (0,0,0) (0,0,1) (0,0,2) (0,1,0) (0,1,1) (0,1,2) (0,2,0) (0,2,1) (0,2,2)
    // map (1,0,0) (1,0,1) (1,0,2) (1,1,0) (1,1,1) (1,1,2) (1,2,0) (1,2,1) (1,2,2)
    // struct (2,0,0) (2,0,1) (2,0,2) (2,1,0) (2,1,1) (2,1,2) (2,2,0) (2,2,1) (2,2,2)
    def table_idx = 0
    for (int i = 0; i < 3; ++i) {
        for (int j = 0; j < 3; ++j) {
            for ( int k = 0; k < 3; ++k) {
                sql """ DROP TABLE IF EXISTS ${table_names_level3[table_idx]} """
                String result = create_table_with_nested_type(3, [i, j, k], table_names_level3[table_idx])
                sql result
                for (int c = 0; c < colNameArr.size(); ++c) {
                    qt_sql_three_level "select count(${colNameArr[c]}) from ${table_names_level3[table_idx]}"
                }
                ++ table_idx
            }
        }
    }


}
