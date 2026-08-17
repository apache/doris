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

suite("test_pythonudtf_forbidden_module") {
    // Test that top-level UDTF module names shadowing server-critical modules
    // are rejected, while a packaged UDTF with a forbidden middle module name still works.

    def pyPath = """${context.file.parent}/udtf_scripts/python_udtf_forbidden_module.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def forbiddenCases = [
        [name: "importlib", function: "py_forbidden_importlib_udtf", symbol: "importlib.forbidden_udtf"],
        [name: "inspect", function: "py_forbidden_inspect_udtf", symbol: "inspect.forbidden_udtf"],
        [name: "ipaddress", function: "py_forbidden_ipaddress_udtf", symbol: "ipaddress.forbidden_udtf"],
        [name: "base64", function: "py_forbidden_base64_udtf", symbol: "base64.forbidden_udtf"],
    ]
    log.info("Python Zip path: ${pyPath}".toString())

    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS udtf_forbidden_test """
        sql """
        CREATE TABLE udtf_forbidden_test (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO udtf_forbidden_test VALUES (1, 10), (2, 20), (3, 30); """

        forbiddenCases.each { forbiddenCase ->
            sql """ DROP FUNCTION IF EXISTS ${forbiddenCase.function}(INT); """
            sql """
            CREATE TABLES FUNCTION ${forbiddenCase.function}(INT)
            RETURNS ARRAY<STRUCT<original:INT, doubled:INT>>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${pyPath}",
                "symbol" = "${forbiddenCase.symbol}",
                "runtime_version" = "${runtime_version}"
            );
            """

            test {
                sql """
                    SELECT tmp.original, tmp.doubled
                    FROM udtf_forbidden_test
                    LATERAL VIEW ${forbiddenCase.function}(val) tmp AS original, doubled
                    ORDER BY id;
                """
                exception "is not allowed for UDFs"
            }
        }

        sql """ DROP FUNCTION IF EXISTS py_mid_forbidden_udtf_ok(INT); """
        sql """
        CREATE TABLES FUNCTION py_mid_forbidden_udtf_ok(INT)
        RETURNS ARRAY<STRUCT<original:INT, shifted:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "safepkg_udtf.inspect.safe_udtf",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_mid_forbidden_udtf_ok """
            SELECT tmp.original, tmp.shifted
            FROM udtf_forbidden_test
            LATERAL VIEW py_mid_forbidden_udtf_ok(val) tmp AS original, shifted
            ORDER BY id;
        """

    } finally {
        forbiddenCases.each { forbiddenCase ->
            try_sql("DROP FUNCTION IF EXISTS ${forbiddenCase.function}(INT);")
        }
        try_sql("DROP FUNCTION IF EXISTS py_mid_forbidden_udtf_ok(INT);")
        try_sql("DROP TABLE IF EXISTS udtf_forbidden_test")
    }
}
