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
suite("test_select_with_predicate_like") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tables=["test_basic_agg"]

    for (String table in tables) {
        sql """drop table if exists ${table};"""
        sql new File("""regression-test/common/table/${table}.sql""").text
        sql new File("""regression-test/common/load/${table}.sql""").text
    }


    qt_select_default  "select 1 from test_basic_agg where    1998  like '1%';"
    qt_select_default2 "select 1 from test_basic_agg where   '1998' like '1%';"
    qt_select_default3 "select 1 from test_basic_agg where    2998  like '1%';"
    qt_select_default4 "select 1 from test_basic_agg where   '2998' like '1%';"
    qt_select_default  "select 1 from test_basic_agg where   199.8  like '1%';"
    qt_select_default2 "select 1 from test_basic_agg where  '199.8' like '1%';"
    qt_select_default3 "select 1 from test_basic_agg where   299.8  like '1%';"
    qt_select_default4 "select 1 from test_basic_agg where  '299.8' like '1%';"
}