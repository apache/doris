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

suite("test_pythonudaf_database_cleanup_retry", "nonConcurrent") {
    def runtime = getPythonUdfRuntimeVersion()
    def originalDb = sql("SELECT DATABASE()")[0][0]
    def heartbeatPoint = "HeartbeatMgr.BackendHeartbeatHandler"
    def zipA = "${context.file.parent}/udaf_scripts/python_udaf_drop_a/python_udaf_drop_test.zip"
    def zipB = "${context.file.parent}/udaf_scripts/python_udaf_drop_b/python_udaf_drop_test.zip"
    [zipA, zipB].each { zip ->
        scp_udf_file_to_all_be(zip)
        scp_udf_file_to_all_fe(zip)
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
        // Remove leftover recycle-bin records before the next case.
        def ids = sql_return_maparray("SHOW CATALOG RECYCLE BIN WHERE NAME = '${db}'")
                .collect { it.DbId }.unique()
        ids.each { id -> sql "DROP CATALOG RECYCLE BIN WHERE 'DbId' = ${id}" }
    }
    def createFunction = { zip, scope ->
        sql """CREATE ${scope} AGGREGATE FUNCTION py_cleanup_retry(INT) RETURNS BIGINT
            PROPERTIES("type"="PYTHON_UDF", "auto_suspend"="0", "symbol"="drop_udaf.SumAgg",
                "file"="file://${zip}", "runtime_version"="${runtime}", "pool_size"="1")"""
    }
    ["force", "recycle", "function", "global"].eachWithIndex { mode, index ->
        def db = "py_udaf_cleanup_retry_${mode}"
        def scope = mode == "global" ? "GLOBAL" : ""
        try {
            if (scope) {
                sql "DROP GLOBAL FUNCTION IF EXISTS py_cleanup_retry(INT)"
            }
            purgeDatabase(db)
            await("earlier database cleanups did not finish", { cleanupTasks().TotalNum.toInteger() == 0 })
            sql "CREATE DATABASE ${db}"
            sql "USE ${db}"
            def dbId = getDbId(db)
            createFunction(zipA, scope)
            "qt_before_${index}"("SELECT py_cleanup_retry(v) FROM (SELECT 1 v UNION ALL SELECT 2 UNION ALL SELECT 3) s")

            def beIds = sql_return_maparray("SHOW BACKENDS").collect { it.BackendId.toString() }
            // Simulate heartbeat failures while Python processes keep running.
            GetDebugPoint().enableDebugPointForAllFEs(heartbeatPoint,
                    [deadBeIds: beIds.join(","), timeout: "180"])
            await("backends were not marked unavailable", {
                sql_return_maparray("SHOW BACKENDS").every { !it.Alive.toBoolean() }
            })
            if (mode in ["function", "global"]) {
                sql "DROP ${scope} FUNCTION py_cleanup_retry(INT)"
            } else if (mode == "recycle") {
                sql "USE ${originalDb}"
                sql "DROP DATABASE ${db}"
                // Recoverable databases retain their function pools.
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
                tasks.TotalNum.toInteger() == beIds.size() && tasks.FailedNum.toInteger() == beIds.size()
            })
            beIds.each { id ->
                def tasks = sql_return_maparray("SHOW PROC '/tasks/CLEAN_UDF_CACHE/${id}'")
                assertEquals(1, tasks.size())
                assertTrue(tasks[0].TaskSignature.toLong() > 0)
            }

            GetDebugPoint().disableDebugPointForAllFEs(heartbeatPoint)
            await("backends did not recover", {
                sql_return_maparray("SHOW BACKENDS").every { it.Alive.toBoolean() }
            })
            // The task leaves the queue only after cleanup succeeds and its result is reported.
            await("backend recovery did not complete function cleanup", {
                cleanupTasks().TotalNum.toInteger() == 0
            })
            if (mode in ["force", "recycle"]) {
                sql "CREATE DATABASE ${db}"
            }
            sql "USE ${db}"
            createFunction(zipB, scope)
            "qt_after_${index}"("SELECT py_cleanup_retry(v) FROM (SELECT 1 v UNION ALL SELECT 2 UNION ALL SELECT 3) s")
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(heartbeatPoint)
            sql "USE ${originalDb}"
            if (scope) {
                sql "DROP GLOBAL FUNCTION IF EXISTS py_cleanup_retry(INT)"
            }
            purgeDatabase(db)
            await("backends did not recover during cleanup", {
                sql_return_maparray("SHOW BACKENDS").every { it.Alive.toBoolean() }
            })
            await("final database cleanups did not finish", { cleanupTasks().TotalNum.toInteger() == 0 })
        }
    }
}
