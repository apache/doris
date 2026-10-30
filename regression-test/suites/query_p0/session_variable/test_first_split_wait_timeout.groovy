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

suite("test_first_split_wait_timeout", "p0") {
    def saved = sql("show variables like 'first_split_wait_timeout_ms'")[0][1]
    try {
        sql "set first_split_wait_timeout_ms=1500"
        order_qt_session "select @@first_split_wait_timeout_ms"
        order_qt_hint "select /*+ SET_VAR(first_split_wait_timeout_ms=2500) */ @@first_split_wait_timeout_ms"
        order_qt_restored "select @@first_split_wait_timeout_ms"
        test {
            sql "set first_split_wait_timeout_ms=0"
            exception "first_split_wait_timeout_ms value should greater than or equal 1"
        }
        test {
            sql "set first_split_wait_timeout_ms=-1"
            exception "first_split_wait_timeout_ms value should greater than or equal 1"
        }
        order_qt_unchanged_after_invalid "select @@first_split_wait_timeout_ms"
    } finally {
        sql "set first_split_wait_timeout_ms=${saved}"
    }
}
