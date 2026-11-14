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
    // UDTF modules named like server modules load from the UDTF package.

    def pyPath = """${context.file.parent}/udtf_scripts/python_udtf_server_module_name.zip"""
    scp_udf_file_to_all_be(pyPath)
    scp_udf_file_to_all_fe(pyPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def functions = [
        [function: "py_server_name_importlib_udtf", symbol: "importlib.double_udtf"],
        [function: "py_server_name_inspect_udtf", symbol: "inspect.double_udtf"],
        [function: "py_server_name_ipaddress_udtf", symbol: "ipaddress.double_udtf"],
        [function: "py_server_name_base64_udtf", symbol: "base64.double_udtf"],
        [function: "py_server_name_types_udtf", symbol: "types.double_udtf"],
        [function: "py_server_name_pkg_inspect_udtf", symbol: "udtfpkg.inspect.shift_udtf"],
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
                "file" = "file://${pyPath}",
                "symbol" = "${udtf.symbol}",
                "runtime_version" = "${runtime_version}"
            );
            """
        }

        qt_importlib """
            SELECT tmp.original, tmp.computed
            FROM udtf_server_module_name_test
            LATERAL VIEW py_server_name_importlib_udtf(val) tmp AS original, computed
            ORDER BY id;
        """
        qt_inspect """
            SELECT tmp.original, tmp.computed
            FROM udtf_server_module_name_test
            LATERAL VIEW py_server_name_inspect_udtf(val) tmp AS original, computed
            ORDER BY id;
        """
        qt_ipaddress """
            SELECT tmp.original, tmp.computed
            FROM udtf_server_module_name_test
            LATERAL VIEW py_server_name_ipaddress_udtf(val) tmp AS original, computed
            ORDER BY id;
        """
        qt_base64 """
            SELECT tmp.original, tmp.computed
            FROM udtf_server_module_name_test
            LATERAL VIEW py_server_name_base64_udtf(val) tmp AS original, computed
            ORDER BY id;
        """
        qt_types """
            SELECT tmp.original, tmp.computed
            FROM udtf_server_module_name_test
            LATERAL VIEW py_server_name_types_udtf(val) tmp AS original, computed
            ORDER BY id;
        """
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
