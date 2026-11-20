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

suite("test_pythonudf_server_module_name") {
    def pyPath = """${context.file.parent}/udf_scripts/python_udf_server_module_name.zip"""
    def nestedPath = """${context.file.parent}/udf_scripts/python_udf_nested_module_name.zip"""
    scp_udf_file_to_all_be(pyPath)
    scp_udf_file_to_all_fe(pyPath)
    scp_udf_file_to_all_be(nestedPath)
    scp_udf_file_to_all_fe(nestedPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def functions = [
        [function: "py_server_name_types", symbol: "types.evaluate", file: pyPath],
        [function: "py_server_name_unused", symbol: "entry.evaluate", file: pyPath],
        [function: "py_server_name_pkg_logging", symbol: "udfpkg.logging.evaluate", file: nestedPath],
        [function: "py_server_name_pkg_resources", symbol: "udfpkg.__init__.evaluate_resources", file: nestedPath],
    ]
    log.info("Python Zip path: ${pyPath}".toString())

    try {
        functions.each { udf ->
            sql """ DROP FUNCTION IF EXISTS ${udf.function}(INT); """
            sql """
            CREATE FUNCTION ${udf.function}(INT)
            RETURNS INT
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "auto_suspend" = "0",
                "pool_size" = "2",
                "file" = "file://${udf.file}",
                "symbol" = "${udf.symbol}",
                "runtime_version" = "${runtime_version}",
                "always_nullable" = "true"
            );
            """
        }

        for (function in ["py_server_name_types", "py_server_name_unused"]) {
            test {
                sql "SELECT ${function}(5)"
                exception "conflicting top-level module names:"
            }
        }

        // Nested names retain ordinary imports, resources and concurrent module caching.
        sql """ DROP TABLE IF EXISTS python_udf_server_module_name_test """
        sql """
        CREATE TABLE python_udf_server_module_name_test (
            id INT,
            val INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES("replication_num" = "1");
        """
        sql """
        INSERT INTO python_udf_server_module_name_test
        SELECT number, number % 100 FROM numbers("number" = "10000");
        """
        qt_concurrent_first_load """
            SELECT SUM(py_server_name_pkg_logging(val)), SUM(py_server_name_pkg_resources(val))
            FROM python_udf_server_module_name_test;
        """

        qt_pkg_logging """ SELECT py_server_name_pkg_logging(10) AS result; """
        qt_pkg_resources """ SELECT py_server_name_pkg_resources(5) AS result; """
    } finally {
        functions.each { udf ->
            try_sql("DROP FUNCTION IF EXISTS ${udf.function}(INT);")
        }
    }
}
