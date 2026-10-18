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

import java.util.concurrent.CountDownLatch

suite("test_pythonudf_native_module_cache") {
    def runtime_version = getPythonUdfRuntimeVersion()
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    scp_udf_file_to_all_fe(pyPath)

    sql """DROP FUNCTION IF EXISTS py_native_module_cache(INT)"""
    sql """
        CREATE FUNCTION py_native_module_cache(INT) RETURNS INT PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "native_module_cache_test.evaluate",
            "runtime_version" = "${runtime_version}",
            "pool_size" = "1"
        )
    """
    sql """DROP TABLE IF EXISTS test_pythonudf_native_module_cache_results"""
    sql """
        CREATE TABLE test_pythonudf_native_module_cache_results (value INT)
        DISTRIBUTED BY HASH(value) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    try {
        // The first call fails if the module directory is removed before evaluate.
        qt_lazy_import """SELECT py_native_module_cache(7)"""
        def ready = new CountDownLatch(4)
        def start = new CountDownLatch(1)
        def workers = (0..<4).collect { worker ->
            thread("native-module-cache-${worker}") {
                ready.countDown()
                start.await()
                3.times {
                    sql """INSERT INTO test_pythonudf_native_module_cache_results
                           SELECT py_native_module_cache(7)"""
                }
            }
        }
        ready.await()
        start.countDown()
        workers.each { it.get() }
        // Every client must observe exactly one execution of module-level code.
        qt_concurrent_import """SELECT COUNT(*), MIN(value), MAX(value)
                                FROM test_pythonudf_native_module_cache_results"""
        qt_repeated_import """SELECT py_native_module_cache(7), py_native_module_cache(NULL)"""
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_native_module_cache(INT)")
    }
}
