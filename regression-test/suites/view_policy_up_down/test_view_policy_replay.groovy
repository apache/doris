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

// Run after load.groovy and an FE restart, as in the other *_up_down suites.
suite("test_view_policy_replay", "p0,view_policy,restart_fe") {
    sql "USE test_view_policy_replay_db"
    sql "DROP ROW POLICY IF EXISTS after_restart ON test_view_policy_replay_db.test_view_policy_grants FOR test_view_policy_user"
    sql "DROP ROW POLICY IF EXISTS min_after_restart ON test_view_policy_replay_db.test_view_policy_min FOR test_view_policy_user"
    qt_desc "DESC test_view_policy_grants"
    sql """CREATE ROW POLICY after_restart ON test_view_policy_replay_db.test_view_policy_grants
        AS RESTRICTIVE TO test_view_policy_user USING (user_name = 'test_view_policy_user')"""
    sql """CREATE ROW POLICY min_after_restart ON test_view_policy_replay_db.test_view_policy_min
        AS RESTRICTIVE TO test_view_policy_user USING (USER_NAME = 'alice')"""
    test {
        sql """CREATE ROW POLICY missing_column ON test_view_policy_replay_db.test_view_policy_grants
            AS RESTRICTIVE TO test_view_policy_user USING (missing = 'alice')"""
        exception "column not exist: missing"
    }
    def url = context.config.jdbcUrl.replaceFirst('/[^/]*$', '/test_view_policy_replay_db?useLocalSessionState=true')
    connectToDoris("test_view_policy_user", "C123_567p", url) {
        order_qt_filtered "SELECT user_name, table_name, privilege FROM test_view_policy_grants"
        order_qt_min_filtered "SELECT * FROM test_view_policy_min"
    }
    connectToDoris("test_view_policy_other", "C123_567p", url) {
        order_qt_other_user "SELECT * FROM test_view_policy_min"
    }
}
