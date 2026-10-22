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

suite("test_pythonudaf_auto_suspend", "nonConcurrent") {
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
            sql """CREATE AGGREGATE FUNCTION py_auto_suspend(INT) RETURNS BIGINT
                PROPERTIES("type"="PYTHON_UDF", "symbol"="SumAgg",
                    "runtime_version"="${runtime}", "pool_size"="1" ${property})
                AS \$\$
class SumAgg:
    def __init__(self):
        self.total = 0
    @property
    def aggregate_state(self):
        return self.total
    def accumulate(self, v):
        if v is not None:
            self.total += v
    def merge(self, other):
        self.total += other
    def finish(self):
        return self.total
\$\$"""
            "qt_result_0_${index}"("SELECT py_auto_suspend(v) FROM py_auto_suspend_input")
            Thread.sleep(2500)
            "qt_result_1_${index}"("SELECT py_auto_suspend(v) FROM py_auto_suspend_input")
            // DROP now leaves a STOPPED pool until the monitor reclaims it. The next
            // iteration recreates the same SQL name with a new function id and must still work.
            sql "DROP FUNCTION py_auto_suspend(INT)"
            test {
                sql "SELECT py_auto_suspend(v) FROM py_auto_suspend_input"
                exception "Can not found function"
            }
        }
        sql "DROP FUNCTION IF EXISTS py_auto_suspend(INT)"
        def zip = "${context.file.parent}/udaf_scripts/python_udaf_drop_a/python_udaf_drop_test.zip"
        scp_udf_file_to_all_be(zip)
        scp_udf_file_to_all_fe(zip)
        sql """CREATE AGGREGATE FUNCTION py_auto_suspend(INT) RETURNS BIGINT
            PROPERTIES("type"="PYTHON_UDF", "symbol"="drop_udaf.SumAgg",
                "file"="file://${zip}", "runtime_version"="${runtime}",
                "pool_size"="1", "auto_suspend"="0")"""
        qt_result_2("SELECT py_auto_suspend(v) FROM py_auto_suspend_input")
        Thread.sleep(2500)
        qt_result_3("SELECT py_auto_suspend(v) FROM py_auto_suspend_input")
        sql "DROP FUNCTION IF EXISTS py_auto_suspend_pid(INT)"
        sql """CREATE AGGREGATE FUNCTION py_auto_suspend_pid(INT) RETURNS BIGINT
            PROPERTIES("type"="PYTHON_UDF", "symbol"="PidAgg",
                "runtime_version"="${runtime}", "pool_size"="1", "auto_suspend"="0")
            AS \$\$
import os
class PidAgg:
    def __init__(self):
        self.total = 0
    @property
    def aggregate_state(self):
        return self.total
    def accumulate(self, v):
        self.total += 1
    def merge(self, other):
        self.total += other
    def finish(self):
        return os.getpid()
\$\$"""
        def queryPid = { sql("SELECT py_auto_suspend_pid(v) FROM py_auto_suspend_input")[0][0] }
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
