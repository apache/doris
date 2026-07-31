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

suite("test_pythonudaf_forbidden_module") {
    // Test that top-level UDAF module names shadowing server-critical modules
    // are rejected, while a packaged UDAF with a forbidden middle module name still works.

    def pyPath = """${context.file.parent}/udaf_scripts/python_udaf_forbidden_module.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def forbiddenCases = [
        [name: "os", function: "py_forbidden_os_udaf", symbol: "os.ForbiddenUDAF"],
        [name: "pathlib", function: "py_forbidden_pathlib_udaf", symbol: "pathlib.ForbiddenUDAF"],
        [name: "pickle", function: "py_forbidden_pickle_udaf", symbol: "pickle.ForbiddenUDAF"],
        [name: "datetime", function: "py_forbidden_datetime_udaf", symbol: "datetime.ForbiddenUDAF"],
    ]
    log.info("Python Zip path: ${pyPath}".toString())

    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS udaf_forbidden_test """
        sql """
        CREATE TABLE udaf_forbidden_test (
            id INT,
            val INT
        ) DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO udaf_forbidden_test VALUES (1, 10), (2, 20), (3, 30); """

        forbiddenCases.each { forbiddenCase ->
            sql """ DROP FUNCTION IF EXISTS ${forbiddenCase.function}(INT); """
            sql """
            CREATE AGGREGATE FUNCTION ${forbiddenCase.function}(INT)
            RETURNS BIGINT
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${pyPath}",
                "symbol" = "${forbiddenCase.symbol}",
                "runtime_version" = "${runtime_version}"
            );
            """

            test {
                sql """ SELECT ${forbiddenCase.function}(val) FROM udaf_forbidden_test; """
                exception "is not allowed for UDFs"
            }
        }

        sql """ DROP FUNCTION IF EXISTS py_mid_forbidden_udaf_ok(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_mid_forbidden_udaf_ok(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "safepkg_udaf.pathlib.SafePathlibUDAF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_mid_forbidden_udaf_ok """ SELECT py_mid_forbidden_udaf_ok(val) AS result FROM udaf_forbidden_test; """

    } finally {
        forbiddenCases.each { forbiddenCase ->
            try_sql("DROP FUNCTION IF EXISTS ${forbiddenCase.function}(INT);")
        }
        try_sql("DROP FUNCTION IF EXISTS py_mid_forbidden_udaf_ok(INT);")
        try_sql("DROP TABLE IF EXISTS udaf_forbidden_test")
    }
}
