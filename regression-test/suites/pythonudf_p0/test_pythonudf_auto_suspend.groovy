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

suite("test_pythonudf_auto_suspend", "nonConcurrent") {
    def runtime = getPythonUdfRuntimeVersion()
    sql "DROP TABLE IF EXISTS py_auto_suspend_input"
    sql """CREATE TABLE py_auto_suspend_input(v INT)
        DUPLICATE KEY(v) DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO py_auto_suspend_input VALUES (1), (2), (3)"
    try {
        // Check results before/after idling; BE unit tests separately verify process termination.
        ["0", "1", "-2", null].eachWithIndex { timeout, index ->
            sql "DROP FUNCTION IF EXISTS py_auto_suspend(INT)"
            def property = timeout == null ? "" : ", \"auto_suspend\"=\"${timeout}\""
            sql """CREATE FUNCTION py_auto_suspend(INT) RETURNS INT
                PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                    "runtime_version"="${runtime}", "pool_size"="1" ${property})
                AS \$\$
def evaluate(v):
    return v + 10 if v is not None else None
\$\$"""
            "qt_result_0_${index}"("SELECT py_auto_suspend(v) FROM py_auto_suspend_input ORDER BY v")
            Thread.sleep(2500)
            "qt_result_1_${index}"("SELECT py_auto_suspend(v) FROM py_auto_suspend_input ORDER BY v")
            // DROP now leaves a STOPPED pool until the monitor reclaims it. The next
            // iteration recreates the same SQL name with a new function id and must still work.
            sql "DROP FUNCTION py_auto_suspend(INT)"
            test {
                sql "SELECT py_auto_suspend(v) FROM py_auto_suspend_input"
                exception "Can not found function"
            }
        }
    } finally {
        sql "DROP FUNCTION IF EXISTS py_auto_suspend(INT)"
    }
}
