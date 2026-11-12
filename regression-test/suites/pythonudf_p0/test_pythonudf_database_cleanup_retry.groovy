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

suite("test_pythonudf_database_cleanup_retry", "nonConcurrent") {
    def runtime = getPythonUdfRuntimeVersion()
    def originalDb = sql("SELECT DATABASE()")[0][0]
    def heartbeatPoint = "HeartbeatMgr.BackendHeartbeatHandler"
    def functions = [
        [type: "udf", kind: "", returns: "INT", symbol: "drop_udf.evaluate",
            query: "SELECT py_cleanup_retry_udf(10)"],
        [type: "udaf", kind: "AGGREGATE", returns: "BIGINT", symbol: "drop_udaf.SumAgg",
            query: "SELECT py_cleanup_retry_udaf(v) FROM (SELECT 1 v UNION ALL SELECT 2 UNION ALL SELECT 3) s"],
        [type: "udtf", kind: "TABLES", returns: "ARRAY<INT>", symbol: "drop_udtf.process",
            query: "SELECT x FROM (SELECT 10 v) s LATERAL VIEW py_cleanup_retry_udtf(v) t AS x "
                + "UNION ALL SELECT x FROM (SELECT 10 v) s LATERAL VIEW py_cleanup_retry_udtf_outer(v) t AS x ORDER BY x"]
    ]
    def zipPath = { type, version ->
        "${context.file.parent}/../python${type}_p0/${type}_scripts/python_${type}_drop_${version}/python_${type}_drop_test.zip"
    }
    functions.each { fn ->
        ["a", "b"].each { version ->
            def zip = zipPath(fn.type, version)
            scp_udf_file_to_all_be(zip)
            scp_udf_file_to_all_fe(zip)
        }
    }
    def await = { String message, Closure ready ->
        def deadline = System.currentTimeMillis() + 120000
        while (!ready()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(message)
            }
            Thread.sleep(1000)
        }
    }
    def cleanupTasks = {
        sql_return_maparray("SHOW PROC '/tasks'").find { it.TaskType == "CLEAN_UDF_CACHE" }
    }
    def purgeDatabase = { db ->
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        def ids = sql_return_maparray("SHOW CATALOG RECYCLE BIN WHERE NAME = '${db}'")
                .collect { it.DbId }.unique()
        ids.each { id -> sql "DROP CATALOG RECYCLE BIN WHERE 'DbId' = ${id}" }
    }
    def createFunctions = { selected, version, scope ->
        selected.each { fn ->
            sql """CREATE ${scope} ${fn.kind} FUNCTION py_cleanup_retry_${fn.type}(INT) RETURNS ${fn.returns}
                PROPERTIES("type"="PYTHON_UDF", "auto_suspend"="-1", "symbol"="${fn.symbol}",
                    "file"="file://${zipPath(fn.type, version)}", "runtime_version"="${runtime}", "pool_size"="1")"""
        }
    }
    ["force", "recycle", "function", "global"].eachWithIndex { mode, index ->
        def db = "py_udf_cleanup_retry_${mode}"
        def scope = mode == "global" ? "GLOBAL" : ""
        def selected = scope ? functions.findAll { it.type != "udtf" } : functions
        try {
            if (scope) {
                selected.each { fn -> sql "DROP GLOBAL FUNCTION IF EXISTS py_cleanup_retry_${fn.type}(INT)" }
            }
            purgeDatabase(db)
            await("earlier database cleanups did not finish", { cleanupTasks().TotalNum.toInteger() == 0 })
            sql "CREATE DATABASE ${db}"
            sql "USE ${db}"
            def dbId = getDbId(db)
            createFunctions(selected, "a", scope)
            def functionCount = sql("SHOW ${scope} FUNCTIONS LIKE 'py_cleanup_retry_%'").size()
            selected.each { fn -> "qt_before_${index}_${fn.type}"(fn.query) }

            def beIds = sql_return_maparray("SHOW BACKENDS").collect { it.BackendId.toString() }
            // Fail heartbeats while Python processes remain alive to exercise cleanup retries.
            GetDebugPoint().enableDebugPointForAllFEs(heartbeatPoint,
                    [deadBeIds: beIds.join(","), timeout: "180"])
            await("backends were not marked unavailable", {
                sql_return_maparray("SHOW BACKENDS").every { !it.Alive.toBoolean() }
            })
            if (mode in ["function", "global"]) {
                selected.each { fn -> sql "DROP ${scope} FUNCTION py_cleanup_retry_${fn.type}(INT)" }
            } else if (mode == "recycle") {
                sql "USE ${originalDb}"
                sql "DROP DATABASE ${db}"
                assertEquals(0, cleanupTasks().TotalNum.toInteger())
                sql "DROP CATALOG RECYCLE BIN WHERE 'DbId' = ${dbId}"
                assertTrue(sql_return_maparray("SHOW CATALOG RECYCLE BIN WHERE NAME = '${db}'")
                        .every { it.DbId.toString() != dbId.toString() })
            } else {
                sql "USE ${originalDb}"
                sql "DROP DATABASE ${db} FORCE"
            }
            await("function cleanup was lost while backend was unavailable", {
                def tasks = cleanupTasks()
                tasks.TotalNum.toInteger() == beIds.size() * functionCount
                        && tasks.FailedNum.toInteger() == beIds.size() * functionCount
            })
            beIds.each { id ->
                def tasks = sql_return_maparray("SHOW PROC '/tasks/CLEAN_UDF_CACHE/${id}'")
                assertEquals(functionCount, tasks.size())
                assertTrue(tasks.every { it.TaskSignature.toLong() > 0 })
            }

            GetDebugPoint().disableDebugPointForAllFEs(heartbeatPoint)
            await("backends did not recover", {
                sql_return_maparray("SHOW BACKENDS").every { it.Alive.toBoolean() }
            })
            await("backend recovery did not complete function cleanup", {
                cleanupTasks().TotalNum.toInteger() == 0
            })
            if (mode in ["force", "recycle"]) {
                sql "CREATE DATABASE ${db}"
            }
            sql "USE ${db}"
            createFunctions(selected, "b", scope)
            selected.each { fn -> "qt_after_${index}_${fn.type}"(fn.query) }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(heartbeatPoint)
            sql "USE ${originalDb}"
            if (scope) {
                selected.each { fn -> sql "DROP GLOBAL FUNCTION IF EXISTS py_cleanup_retry_${fn.type}(INT)" }
            }
            purgeDatabase(db)
            await("backends did not recover during cleanup", {
                sql_return_maparray("SHOW BACKENDS").every { it.Alive.toBoolean() }
            })
            await("final database cleanups did not finish", { cleanupTasks().TotalNum.toInteger() == 0 })
        }
    }
}
