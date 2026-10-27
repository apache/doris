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

suite("test_pythonudaf_server_module_name") {
    // UDAF modules named like server modules, with state objects merged across workers.

    def pyPath = """${context.file.parent}/udaf_scripts/python_udaf_server_module_name.zip"""
    scp_udf_file_to_all_be(pyPath)
    scp_udf_file_to_all_fe(pyPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    def functions = [
        [function: "py_server_name_os_udaf", symbol: "os.SumUDAF"],
        [function: "py_server_name_pathlib_udaf", symbol: "pathlib.SumUDAF"],
        [function: "py_server_name_pickle_udaf", symbol: "pickle.SumUDAF"],
        [function: "py_server_name_datetime_udaf", symbol: "datetime.SumUDAF"],
        [function: "py_server_name_pkg_pathlib_udaf", symbol: "udafpkg.pathlib.PackagedSumUDAF"],
        [function: "py_server_name_types_state_udaf", symbol: "types.StateObjectUDAF"],
        [function: "py_server_name_json_state_udaf", symbol: "json.udaf.MaxUDAF"],
    ]
    log.info("Python Zip path: ${pyPath}".toString())

    try {
        sql """ DROP TABLE IF EXISTS udaf_server_module_name_test """
        sql """
        CREATE TABLE udaf_server_module_name_test (
            id INT,
            grp INT,
            val INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES("replication_num" = "1");
        """
        sql """
        INSERT INTO udaf_server_module_name_test
        SELECT number, number % 5, number % 100 FROM numbers("number" = "10000");
        """

        functions.each { udaf ->
            sql """ DROP FUNCTION IF EXISTS ${udaf.function}(INT); """
            sql """
            CREATE AGGREGATE FUNCTION ${udaf.function}(INT)
            RETURNS BIGINT
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "auto_suspend" = "0",
                "pool_size" = "4",
                "file" = "file://${pyPath}",
                "symbol" = "${udaf.symbol}",
                "runtime_version" = "${runtime_version}"
            );
            """
        }

        order_qt_sum_states """
            SELECT grp,
                   py_server_name_os_udaf(val),
                   py_server_name_pathlib_udaf(val),
                   py_server_name_pickle_udaf(val),
                   py_server_name_datetime_udaf(val),
                   py_server_name_pkg_pathlib_udaf(val)
            FROM udaf_server_module_name_test
            GROUP BY grp;
        """

        order_qt_object_states """
            SELECT grp,
                   py_server_name_types_state_udaf(val),
                   py_server_name_json_state_udaf(id)
            FROM udaf_server_module_name_test
            GROUP BY grp;
        """

        qt_object_states_without_group """
            SELECT py_server_name_types_state_udaf(val), py_server_name_json_state_udaf(id)
            FROM udaf_server_module_name_test;
        """
    } finally {
        functions.each { udaf ->
            try_sql("DROP FUNCTION IF EXISTS ${udaf.function}(INT);")
        }
    }
}
