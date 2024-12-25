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

suite("test_nereids_workload_test") {
    sql "drop workload group if exists test_nereids_wg1;"
    sql "drop workload group if exists test_nereids_wg2;"
    checkNereidsExecute("create workload group test_nereids_wg1 properties('cpu_share'='1024');")
    checkNereidsExecute("create workload group test_nereids_wg2 properties('cpu_share'='1024');")
    qt_check_workload_check1("select NAME from information_schema.workload_groups where NAME='test_nereids_wg1';")
    checkNereidsExecute("drop workload group  test_nereids_wg1;")
    qt_check_workload_check2("select NAME from information_schema.workload_groups where NAME='test_nereids_wg1';")
    checkNereidsExecute("drop workload group if exists test_nereids_wg2;")
}
