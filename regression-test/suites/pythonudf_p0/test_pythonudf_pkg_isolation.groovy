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

suite("test_pythonudf_pkg_isolation") {
    def runtime_version = getPythonUdfRuntimeVersion()
    def zipA = """${context.file.parent}/udf_scripts/python_udf_pkg_a/python_udf_pkg_test.zip"""
    def zipB = """${context.file.parent}/udf_scripts/python_udf_pkg_b/python_udf_pkg_test.zip"""

    scp_udf_file_to_all_be(zipA)
    scp_udf_file_to_all_be(zipB)

    try {
        // Case 1: Same package, same module, different zip paths
        sql """DROP FUNCTION IF EXISTS py_pkg_a_mod_x(INT)"""
        sql """DROP FUNCTION IF EXISTS py_pkg_b_mod_x(INT)"""
        sql """
            CREATE FUNCTION py_pkg_a_mod_x(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "mypkg.mod_x.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """
        sql """
            CREATE FUNCTION py_pkg_b_mod_x(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "mypkg.mod_x.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_1 """SELECT py_pkg_a_mod_x(5), py_pkg_b_mod_x(5);"""

        // Case 2: Same package, different modules, same zip
        sql """DROP FUNCTION IF EXISTS py_pkg_a_mod_y(INT)"""
        sql """
            CREATE FUNCTION py_pkg_a_mod_y(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "mypkg.mod_y.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_2 """SELECT py_pkg_a_mod_x(5), py_pkg_a_mod_y(5);"""

        // Case 3: Same package, different modules, different zips
        sql """DROP FUNCTION IF EXISTS py_pkg_b_mod_y(INT)"""
        sql """
            CREATE FUNCTION py_pkg_b_mod_y(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "mypkg.mod_y.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_3 """SELECT py_pkg_a_mod_y(5), py_pkg_b_mod_y(5);"""

        // Case 4: All four combinations together
        qt_pkg_isolation_4 """SELECT py_pkg_a_mod_x(10), py_pkg_a_mod_y(10), py_pkg_b_mod_x(10), py_pkg_b_mod_y(10);"""

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_pkg_a_mod_x(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_pkg_a_mod_y(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_pkg_b_mod_x(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_pkg_b_mod_y(INT);")
    }
}
