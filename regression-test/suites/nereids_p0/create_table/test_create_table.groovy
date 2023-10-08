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

// this suite is for creating table with timestamp datatype in defferent 
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("nereids_create_table") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'

    def str = new File("""${context.file.parent}/ddl/table.sql""").text
    for (String table in str.split(';')) {
        sql table
        sleep(100)
    }

    str = new File("""${context.file.parent}/ddl/data.sql""").text
    for (String table in str.split(';')) {
        sql table
        sleep(100)
    }
    sql 'sync'
    def tables = ['test_all_types', 'test_agg_key', 'test_uni_key', 'test_uni_key_mow', 'test_not_null',
                  'test_random', 'test_random_auto', 'test_less_than_partition', 'test_range_partition',
                  'test_step_partition', 'test_date_step_partition', 'test_list_partition', 'test_rollup',
                  'test_default_value']
    for (String t in tables) {
        qt_sql "select * from ${t} order by id"
    }
}
