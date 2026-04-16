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

suite("nereids_scalar_fn_typeof") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    qt_sql_typeof_bigint "select typeof(cast(1 as bigint))"
    qt_sql_typeof_decimal "select typeof(cast(1.23 as decimal(10,2)))"
    qt_sql_typeof_array "select typeof(array(1, 2))"
    qt_sql_typeof_struct "select typeof(named_struct('id', 1, 'name', 'a'))"
}
