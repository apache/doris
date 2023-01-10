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

suite("rlike") {
    sql "use regression_test_nereids_syntax_p0"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    qt_regexp "select c_name from customer where c_name regexp '9' order by c_custkey"
    qt_rlike "select c_name from customer where c_name rlike '9' order by c_custkey"
    qt_not_regexp "select c_name from customer where c_name not regexp '9' order by c_custkey"
    qt_not_rlike "select c_name from customer where c_name not rlike '9' order by c_custkey"
}