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

suite("test_complextype_information_schema") {
    // add array/map/struct
    def table_names = ["array_info", "map_info", "struct_info"]
    for (int i = 0; i < table_names.size(); ++i) {
        sql """ DROP TABLE IF EXISTS ${table_names[i]} """
        String result = create_table_with_nested_type(1, [i], table_names[i])
        sql result
    }

    for (int i = 0; i < table_names.size(); ++i) {
        sql "use information_schema"
        qt_sql "select data_type, column_type from columns where TABLE_NAME=\"${table_names[i]}\";"
    }
}
