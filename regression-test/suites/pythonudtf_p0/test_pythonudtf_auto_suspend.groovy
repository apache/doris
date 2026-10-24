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

suite("test_pythonudtf_auto_suspend", "nonConcurrent") {
    def runtime = getPythonUdfRuntimeVersion()
    sql "DROP TABLE IF EXISTS py_auto_suspend_input"
    sql """CREATE TABLE py_auto_suspend_input(v INT)
        DUPLICATE KEY(v) DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO py_auto_suspend_input VALUES (1), (2), (3)"
    try {
        // Check results before/after idling; BE unit tests separately verify process termination.
        ["0", "1", "-1", null].eachWithIndex { timeout, index ->
            sql "DROP FUNCTION IF EXISTS py_auto_suspend(INT)"
            def property = timeout == null ? "" : ", \"auto_suspend\"=\"${timeout}\""
            sql """CREATE TABLES FUNCTION py_auto_suspend(INT) RETURNS ARRAY<INT>
                PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                    "runtime_version"="${runtime}", "pool_size"="1" ${property})
                AS \$\$
def evaluate(v):
    if v is not None:
        yield (v + 10,)
\$\$"""
            "qt_result_0_${index}"("SELECT x FROM py_auto_suspend_input LATERAL VIEW py_auto_suspend(v) t AS x ORDER BY x")
            "qt_result_1_${index}"("SELECT x FROM (SELECT CAST(NULL AS INT) v) s LATERAL VIEW py_auto_suspend_outer(v) t AS x")
            Thread.sleep(2500)
            "qt_result_2_${index}"("SELECT x FROM py_auto_suspend_input LATERAL VIEW py_auto_suspend(v) t AS x ORDER BY x")
            "qt_result_3_${index}"("SELECT x FROM (SELECT CAST(NULL AS INT) v) s LATERAL VIEW py_auto_suspend_outer(v) t AS x")
            // DROP now leaves a STOPPED pool until the monitor reclaims it. The next
            // iteration recreates the same SQL name with a new function id and must still work.
            sql "DROP FUNCTION py_auto_suspend(INT)"
            test {
                sql "SELECT x FROM py_auto_suspend_input LATERAL VIEW py_auto_suspend(v) t AS x"
                exception "Can not found function"
            }
        }
        sql "DROP FUNCTION IF EXISTS py_auto_suspend_pid(INT)"
        sql """CREATE TABLES FUNCTION py_auto_suspend_pid(INT) RETURNS ARRAY<BIGINT>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                "runtime_version"="${runtime}", "pool_size"="1", "auto_suspend"="0")
            AS \$\$
import os
def evaluate(v):
    yield (os.getpid(),)
\$\$"""
        def queryPid = {
            sql("SELECT x FROM py_auto_suspend_input LATERAL VIEW py_auto_suspend_pid(v) t AS x")[0][0]
        }
        def firstPid = queryPid()
        def nextPid = firstPid
        def deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(120)
        while (nextPid == firstPid && System.nanoTime() < deadline) {
            Thread.sleep(2000)
            nextPid = queryPid()
        }
        assertNotEquals(firstPid, nextPid)
    } finally {
        sql "DROP FUNCTION IF EXISTS py_auto_suspend(INT)"
        sql "DROP FUNCTION IF EXISTS py_auto_suspend_pid(INT)"
    }
}
