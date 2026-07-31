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

suite("test_pythonudtf_pkg_isolation") {
    def runtime_version = getPythonUdfRuntimeVersion()
    def zipA = """${context.file.parent}/udtf_scripts/python_udtf_pkg_a/python_udtf_pkg_test.zip"""
    def zipB = """${context.file.parent}/udtf_scripts/python_udtf_pkg_b/python_udtf_pkg_test.zip"""

    scp_udf_file_to_all_be(zipA)
    scp_udf_file_to_all_be(zipB)

    sql """DROP TABLE IF EXISTS py_udtf_pkg_tbl"""
    sql """
        CREATE TABLE py_udtf_pkg_tbl (
            v INT
        ) ENGINE=OLAP
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql """INSERT INTO py_udtf_pkg_tbl VALUES (1), (2);"""

    try {
        // Case 1: Same package, same module, different zip paths
        sql """DROP FUNCTION IF EXISTS py_pkg_a_t_x(INT)"""
        sql """DROP FUNCTION IF EXISTS py_pkg_b_t_x(INT)"""
        sql """
            CREATE TABLES FUNCTION py_pkg_a_t_x(INT)
            RETURNS ARRAY<INT>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "mypkg.mod_x.process",
                "runtime_version" = "${runtime_version}"
            )
        """
        sql """
            CREATE TABLES FUNCTION py_pkg_b_t_x(INT)
            RETURNS ARRAY<INT>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "mypkg.mod_x.process",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_1 """
            SELECT a.c, b.c
            FROM py_udtf_pkg_tbl
            LATERAL VIEW py_pkg_a_t_x(v) a AS c
            LATERAL VIEW py_pkg_b_t_x(v) b AS c
            ORDER BY a.c, b.c;
        """

        // Case 2: Same package, different modules, same zip
        sql """DROP FUNCTION IF EXISTS py_pkg_a_t_y(INT)"""
        sql """
            CREATE TABLES FUNCTION py_pkg_a_t_y(INT)
            RETURNS ARRAY<INT>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "mypkg.mod_y.process",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_2 """
            SELECT a.c, b.c
            FROM py_udtf_pkg_tbl
            LATERAL VIEW py_pkg_a_t_x(v) a AS c
            LATERAL VIEW py_pkg_a_t_y(v) b AS c
            ORDER BY a.c, b.c;
        """

        // Case 3: Same package, different modules, different zips
        sql """DROP FUNCTION IF EXISTS py_pkg_b_t_y(INT)"""
        sql """
            CREATE TABLES FUNCTION py_pkg_b_t_y(INT)
            RETURNS ARRAY<INT>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "mypkg.mod_y.process",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_3 """
            SELECT a.c, b.c
            FROM py_udtf_pkg_tbl
            LATERAL VIEW py_pkg_a_t_y(v) a AS c
            LATERAL VIEW py_pkg_b_t_y(v) b AS c
            ORDER BY a.c, b.c;
        """

        // Case 4: All four combinations together
        qt_pkg_isolation_4 """
            SELECT ax.c, ay.c, bx.c, b_y.c
            FROM py_udtf_pkg_tbl
            LATERAL VIEW py_pkg_a_t_x(v) ax AS c
            LATERAL VIEW py_pkg_a_t_y(v) ay AS c
            LATERAL VIEW py_pkg_b_t_x(v) bx AS c
            LATERAL VIEW py_pkg_b_t_y(v) b_y AS c
            ORDER BY ax.c, ay.c, bx.c, b_y.c;
        """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_pkg_a_t_x(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_pkg_a_t_y(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_pkg_b_t_x(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_pkg_b_t_y(INT);")
    }
}
