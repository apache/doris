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

suite("test_audit_interval_bounds", "p0,nonConcurrent") {
    def original = sql("show global variables like 'audit_plugin_max_batch_interval_sec'")[0][1]
    try {
        // SET alone reproduces the overflow risk. No audit table or loaded rows are needed.
        // Check the first overflowing second and LONG_MAX, which would become -1000 ms.
        ["9223372036854776", "9223372036854775807"].each { value ->
            test {
                sql "set global audit_plugin_max_batch_interval_sec = ${value}"
                exception "must be"
            }
        }

        // Keep both valid boundaries. Zero flushes pending data without an interval wait.
        sql "set global audit_plugin_max_batch_interval_sec = 0"
        order_qt_zero "show global variables like 'audit_plugin_max_batch_interval_sec'"
        sql "set global audit_plugin_max_batch_interval_sec = 9223372036854775"
        order_qt_max "show global variables like 'audit_plugin_max_batch_interval_sec'"
    } finally {
        // Restore the value even when an unfixed FE accepts an invalid input.
        sql "set global audit_plugin_max_batch_interval_sec = ${original}"
    }
}
