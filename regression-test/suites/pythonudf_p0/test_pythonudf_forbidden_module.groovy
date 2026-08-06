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

suite("test_pythonudf_forbidden_module") {
    // Test that top-level UDF module names shadowing server-critical modules
    // are rejected, while a packaged UDF with a forbidden middle module name still works.

    def pyPath = """${context.file.parent}/udf_scripts/python_udf_forbidden_module.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def forbiddenCases = [
        [name: "threading", function: "py_forbidden_threading", symbol: "threading.evaluate"],
        [name: "json", function: "py_forbidden_json", symbol: "json.evaluate"],
        [name: "sys", function: "py_forbidden_sys", symbol: "sys.evaluate"],
        [name: "logging", function: "py_forbidden_logging", symbol: "logging.evaluate"],
    ]
    log.info("Python Zip path: ${pyPath}".toString())

    try {
        forbiddenCases.each { forbiddenCase ->
            sql """ DROP FUNCTION IF EXISTS ${forbiddenCase.function}(INT); """
            sql """
            CREATE FUNCTION ${forbiddenCase.function}(INT)
            RETURNS INT
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${pyPath}",
                "symbol" = "${forbiddenCase.symbol}",
                "runtime_version" = "${runtime_version}",
                "always_nullable" = "true"
            );
            """

            test {
                sql """ SELECT ${forbiddenCase.function}(1); """
                exception "is not allowed for UDFs"
            }
        }

        sql """ DROP FUNCTION IF EXISTS py_mid_forbidden_ok(INT); """
        sql """
        CREATE FUNCTION py_mid_forbidden_ok(INT)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "safepkg_udf.logging.evaluate",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """

        qt_mid_forbidden_ok """ SELECT py_mid_forbidden_ok(10) AS result; """

    } finally {
        forbiddenCases.each { forbiddenCase ->
            try_sql("DROP FUNCTION IF EXISTS ${forbiddenCase.function}(INT);")
        }
        try_sql("DROP FUNCTION IF EXISTS py_mid_forbidden_ok(INT);")
    }
}
