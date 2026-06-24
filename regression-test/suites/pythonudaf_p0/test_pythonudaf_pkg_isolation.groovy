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

suite('test_pythonudaf_pkg_isolation') {
    def runtime_version = getPythonUdfRuntimeVersion()
    def zipA = """${context.file.parent}/udaf_scripts/python_udaf_pkg_a/python_udaf_pkg_test.zip"""
    def zipB = """${context.file.parent}/udaf_scripts/python_udaf_pkg_b/python_udaf_pkg_test.zip"""

    scp_udf_file_to_all_be(zipA)
    scp_udf_file_to_all_be(zipB)

    sql '''DROP TABLE IF EXISTS py_udaf_pkg_tbl'''
    sql '''
        CREATE TABLE py_udaf_pkg_tbl (
            v INT
        ) ENGINE=OLAP
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    '''
    sql '''INSERT INTO py_udaf_pkg_tbl VALUES (1), (2), (3);'''

    try {
        // Case 1: Same package, same module, different zip paths
        sql '''DROP FUNCTION IF EXISTS py_pkg_a_sum_x(INT)'''
        sql '''DROP FUNCTION IF EXISTS py_pkg_b_sum_x(INT)'''
        sql """
            CREATE AGGREGATE FUNCTION py_pkg_a_sum_x(INT) RETURNS BIGINT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "mypkg.mod_x.SumAgg",
                "runtime_version" = "${runtime_version}"
            )
        """
        sql """
            CREATE AGGREGATE FUNCTION py_pkg_b_sum_x(INT) RETURNS BIGINT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "mypkg.mod_x.SumAgg",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_1 '''SELECT py_pkg_a_sum_x(v), py_pkg_b_sum_x(v) FROM py_udaf_pkg_tbl;'''

        // Case 2: Same package, different modules, same zip
        sql '''DROP FUNCTION IF EXISTS py_pkg_a_sum_y(INT)'''
        sql """
            CREATE AGGREGATE FUNCTION py_pkg_a_sum_y(INT) RETURNS BIGINT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "mypkg.mod_y.SumAgg",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_2 '''SELECT py_pkg_a_sum_x(v), py_pkg_a_sum_y(v) FROM py_udaf_pkg_tbl;'''

        // Case 3: Same package, different modules, different zips
        sql '''DROP FUNCTION IF EXISTS py_pkg_b_sum_y(INT)'''
        sql """
            CREATE AGGREGATE FUNCTION py_pkg_b_sum_y(INT) RETURNS BIGINT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "mypkg.mod_y.SumAgg",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_pkg_isolation_3 '''SELECT py_pkg_a_sum_y(v), py_pkg_b_sum_y(v) FROM py_udaf_pkg_tbl;'''

        // Case 4: All four combinations together
        qt_pkg_isolation_4 '''SELECT py_pkg_a_sum_x(v), py_pkg_a_sum_y(v), py_pkg_b_sum_x(v), py_pkg_b_sum_y(v) FROM py_udaf_pkg_tbl;'''

    } finally {
        try_sql('DROP FUNCTION IF EXISTS py_pkg_a_sum_x(INT);')
        try_sql('DROP FUNCTION IF EXISTS py_pkg_a_sum_y(INT);')
        try_sql('DROP FUNCTION IF EXISTS py_pkg_b_sum_x(INT);')
        try_sql('DROP FUNCTION IF EXISTS py_pkg_b_sum_y(INT);')
    }
}
