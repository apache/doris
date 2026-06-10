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

suite("test_now_consistent") {
    sql "set enable_fold_nondeterministic_fn=true"

    def nowQueries = (1..100).collect {
        it % 2 == 0 ? "select now(6) as ts" : "select current_timestamp(6) as ts"
    }.join(" union all ")
    qt_now_consistent "select count(distinct ts) from (${nowQueries}) t"

    sql "set time_zone='+08:00'"
    qt_set_var_time_zone """
        select /*+ SET_VAR(time_zone='+00:00') */
            abs(unix_timestamp(now()) - unix_timestamp()) <= 5
    """
}
