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

suite("test_external_scan_consistent_hash_spread_variable", "p0") {
    order_qt_default "show session variables like 'external_scan_consistent_hash_spread_num'"
    sql "set external_scan_consistent_hash_spread_num = 3"
    order_qt_enabled "show session variables like 'external_scan_consistent_hash_spread_num'"
    sql "set external_scan_consistent_hash_spread_num = 2147483647"
    order_qt_maximum "show session variables like 'external_scan_consistent_hash_spread_num'"

    test {
        sql "set external_scan_consistent_hash_spread_num = 0"
        exception "greater than or equal 1"
    }
    test {
        sql "set external_scan_consistent_hash_spread_num = -1"
        exception "greater than or equal 1"
    }
    test {
        sql "set external_scan_consistent_hash_spread_num = 2147483648"
        exception "2147483648"
    }
    test {
        sql "set external_scan_consistent_hash_spread_num = 'abc'"
        exception "abc"
    }
    order_qt_after_errors "show session variables like 'external_scan_consistent_hash_spread_num'"
    sql "unset variable external_scan_consistent_hash_spread_num"
    order_qt_unset "show session variables like 'external_scan_consistent_hash_spread_num'"
}
