
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

suite("test_date_acquire") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set enable_fold_constant_by_be=true"

    explain {
        sql 'select now(), now(3), curdate(), current_date(), curtime(), current_time(), current_timestamp(), current_timestamp(3)'
        contains('')
    }

    qt_sql11 "select from_unixtime(1553152255)"
    qt_sql12 "select unix_timestamp(1553152255)"

    sql "set time_zone='+00:00'"

    qt_sql21 "select from_unixtime(1553152255)"
    qt_sql22 "select unix_timestamp(1553152255)"

    sql "set time_zone='+04:00'"

    qt_sql31 "select from_unixtime(1553152255)"
    qt_sql32 "select unix_timestamp(1553152255)"
}
