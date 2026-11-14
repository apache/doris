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
    // UDF modules named like server modules load from the UDF package.

    def pyPath = """${context.file.parent}/udf_scripts/python_udf_server_module_name.zip"""
    scp_udf_file_to_all_be(pyPath)
    scp_udf_file_to_all_fe(pyPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def functions = [
        [function: "py_server_name_types", symbol: "types.evaluate"],
        [function: "py_server_name_json", symbol: "json.evaluate"],
        [function: "py_server_name_logging", symbol: "logging.evaluate"],
        [function: "py_server_name_sys", symbol: "sys.evaluate"],
        [function: "py_server_name_threading", symbol: "threading.evaluate"],
        [function: "py_server_name_pkg_logging", symbol: "udfpkg.logging.evaluate"],
        [function: "py_server_name_uuid_pkg", symbol: "uuid.mod.evaluate"],
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
                "file" = "file://${pyPath}",
                "symbol" = "${udf.symbol}",
                "runtime_version" = "${runtime_version}",
                "always_nullable" = "true"
            );
            """
        }

        // Runs first to cover concurrent first imports.
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
            SELECT SUM(py_server_name_uuid_pkg(val)), SUM(py_server_name_types(val))
            FROM python_udf_server_module_name_test;
        """

        qt_types """ SELECT py_server_name_types(5) AS result; """
        qt_json """ SELECT py_server_name_json(1) AS result; """
        qt_logging """ SELECT py_server_name_logging(1) AS result; """
        qt_sys """ SELECT py_server_name_sys(1) AS result; """
        qt_threading """ SELECT py_server_name_threading(1) AS result; """
        qt_pkg_logging """ SELECT py_server_name_pkg_logging(10) AS result; """
        qt_uuid_pkg """ SELECT py_server_name_uuid_pkg(1) AS result; """
    } finally {
        functions.each { udf ->
            try_sql("DROP FUNCTION IF EXISTS ${udf.function}(INT);")
        }
    }
}
