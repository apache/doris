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

suite("test_pythonudtf_server_module_name") {
    def pyPath = """${context.file.parent}/udtf_scripts/python_udtf_server_module_name.zip"""
    def nestedPath = """${context.file.parent}/udtf_scripts/python_udtf_nested_module_name.zip"""
    scp_udf_file_to_all_be(pyPath)
    scp_udf_file_to_all_fe(pyPath)
    scp_udf_file_to_all_be(nestedPath)
    scp_udf_file_to_all_fe(nestedPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def functions = [
        [function: "py_server_name_importlib_udtf", symbol: "importlib.double_udtf", file: pyPath],
        [function: "py_server_name_pkg_inspect_udtf", symbol: "udtfpkg.inspect.shift_udtf", file: nestedPath],
    ]
    log.info("Python Zip path: ${pyPath}".toString())

    try {
        sql """ DROP TABLE IF EXISTS udtf_server_module_name_test """
        sql """
        CREATE TABLE udtf_server_module_name_test (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO udtf_server_module_name_test VALUES (1, 10), (2, 20), (3, 30); """

        functions.each { udtf ->
            sql """ DROP FUNCTION IF EXISTS ${udtf.function}(INT); """
            sql """
            CREATE TABLES FUNCTION ${udtf.function}(INT)
            RETURNS ARRAY<STRUCT<original:INT, computed:INT>>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "auto_suspend" = "0",
                "pool_size" = "1",
                "file" = "file://${udtf.file}",
                "symbol" = "${udtf.symbol}",
                "runtime_version" = "${runtime_version}"
            );
            """
        }

        test {
            sql """SELECT tmp.original, tmp.computed
                   FROM udtf_server_module_name_test
                   LATERAL VIEW py_server_name_importlib_udtf(val) tmp AS original, computed"""
            exception "conflicting top-level module names:"
        }
        qt_pkg_inspect """
            SELECT tmp.original, tmp.computed
            FROM udtf_server_module_name_test
            LATERAL VIEW py_server_name_pkg_inspect_udtf(val) tmp AS original, computed
            ORDER BY id;
        """
    } finally {
        functions.each { udtf ->
            try_sql("DROP FUNCTION IF EXISTS ${udtf.function}(INT);")
        }
    }
}
