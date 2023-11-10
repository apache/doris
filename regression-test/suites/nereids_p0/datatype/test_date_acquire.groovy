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
    sql 'set enable_fold_nondeterministic_fn=true'

    String res = sql 'explain select now(), now(3), curdate(), current_date(), current_timestamp(), current_timestamp(3)'
    res = res.split('VUNION')[1]
    assertFalse(res.contains("()") || res.contains("(3)"))

    sql "set enable_fold_constant_by_be=true"

    test {
        sql "select from_unixtime(1553152255), unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')"
        result([['2019-03-21 15:10:55', 1196389819]])
    }

    sql "set time_zone='+00:00'"

    test {
        sql "select from_unixtime(1553152255), unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')"
        result([['2019-03-21 07:10:55', 1196418619]])
    }

    sql "set time_zone='+04:00'"

    test {
        sql "select from_unixtime(1553152255), unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')"
        result([['2019-03-21 11:10:55', 1196404219]])
    }
}
