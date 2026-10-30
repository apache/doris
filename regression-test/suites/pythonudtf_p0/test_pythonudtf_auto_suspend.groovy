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
        sql "DROP FUNCTION IF EXISTS py_auto_suspend(INT)"
        sql """CREATE TABLES FUNCTION py_auto_suspend(INT) RETURNS ARRAY<STRING>
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate",
                "runtime_version"="${runtime}", "pool_size"="1", "auto_suspend"="0")
            AS \$\$
import os
def evaluate(v):
    if v is not None:
        yield (f"{os.getpid()}:{v + 10}",)
\$\$"""
        def query = "SELECT x FROM py_auto_suspend_input LATERAL VIEW py_auto_suspend(v) t AS x"
        def queryPid = { sql("SELECT split_part(x, ':', 1) FROM (${query}) s")[0][0] }
        def resultQuery = "SELECT split_part(x, ':', 2) FROM (${query}) s ORDER BY 1"
        def outerQuery = "SELECT x FROM (SELECT CAST(NULL AS INT) v) s LATERAL VIEW py_auto_suspend_outer(v) t AS x"
        qt_before(resultQuery)
        qt_outer_before(outerQuery)
        def firstPid = queryPid()
        def nextPid = firstPid
        def deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(120)
        while (nextPid == firstPid && System.nanoTime() < deadline) {
            Thread.sleep(2000)
            nextPid = queryPid()
        }
        assertNotEquals(firstPid, nextPid)
        qt_after(resultQuery)
        qt_outer_after(outerQuery)
    } finally {
        sql "DROP FUNCTION IF EXISTS py_auto_suspend(INT)"
    }
}
