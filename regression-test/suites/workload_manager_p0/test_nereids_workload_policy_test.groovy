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

suite("test_nereids_workload_policy_test") {
    sql "drop workload policy if exists test_nereids_worklod_policy1;"
    sql "create workload policy test_nereids_worklod_policy1 " +
            "conditions(username='root') " +
            "actions(set_session_variable 'workload_group=normal') " +
            "properties( " +
            "'enabled' = 'false', " +
            "'priority'='10' " +
            ");"
    qt_check_workload_policy_check1("select NAME from information_schema.workload_policy where NAME='test_nereids_worklod_policy1';")
    checkNereidsExecute("drop workload policy  test_nereids_worklod_policy1;")
    checkNereidsExecute("drop workload policy if exists test_nereids_worklod_policy1;")
    qt_check_workload_policy_check2("select NAME from information_schema.workload_policy where NAME='test_nereids_worklod_policy1';")

}