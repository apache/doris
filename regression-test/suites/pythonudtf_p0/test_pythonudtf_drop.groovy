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

suite("test_pythonudtf_drop") {
    def runtime_version = "3.8.10"
    def zipA = """${context.file.parent}/udtf_scripts/python_udtf_drop_a/python_udtf_drop_test.zip"""
    def zipB = """${context.file.parent}/udtf_scripts/python_udtf_drop_b/python_udtf_drop_test.zip"""

    scp_udf_file_to_all_be(zipA)
    scp_udf_file_to_all_be(zipB)

    sql """DROP TABLE IF EXISTS py_udtf_drop_tbl"""
    sql """
        CREATE TABLE py_udtf_drop_tbl (
            v INT
        ) ENGINE=OLAP
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql """INSERT INTO py_udtf_drop_tbl VALUES (1), (2);"""

    try {
        // Case 1: simple drop should make subsequent call fail
        sql """DROP FUNCTION IF EXISTS py_drop_t_once(INT)"""
        sql """
            CREATE TABLES FUNCTION py_drop_t_once(INT)
            RETURNS ARRAY<INT>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "drop_udtf.process",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_py_udtf_drop_1 """
            SELECT c
            FROM py_udtf_drop_tbl
            LATERAL VIEW py_drop_t_once(v) tmp AS c
            ORDER BY c;
        """
        try_sql("DROP FUNCTION IF EXISTS py_drop_t_once(INT);")
        test {
            sql """
                SELECT c
                FROM py_udtf_drop_tbl
                LATERAL VIEW py_drop_t_once(v) tmp AS c;
            """
            exception "Can not found function"
        }

        // Case 2: same module name, different file paths
        sql """DROP FUNCTION IF EXISTS py_drop_t_a(INT)"""
        sql """DROP FUNCTION IF EXISTS py_drop_t_b(INT)"""
        sql """
            CREATE TABLES FUNCTION py_drop_t_a(INT)
            RETURNS ARRAY<INT>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "drop_udtf.process",
                "runtime_version" = "${runtime_version}"
            )
        """
        sql """
            CREATE TABLES FUNCTION py_drop_t_b(INT)
            RETURNS ARRAY<INT>
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "drop_udtf.process",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_py_udtf_drop_2 """
            SELECT a_tmp.c, b_tmp.c
            FROM py_udtf_drop_tbl
            LATERAL VIEW py_drop_t_a(v) a_tmp AS c
            LATERAL VIEW py_drop_t_b(v) b_tmp AS c
            ORDER BY a_tmp.c, b_tmp.c;
        """

        try_sql("DROP FUNCTION IF EXISTS py_drop_t_b(INT);")
        test {
            sql """
                SELECT c
                FROM py_udtf_drop_tbl
                LATERAL VIEW py_drop_t_b(v) tmp AS c;
            """
            exception "Can not found function"
        }

        qt_py_udtf_drop_3 """
            SELECT c
            FROM py_udtf_drop_tbl
            LATERAL VIEW py_drop_t_a(v) tmp AS c
            ORDER BY c;
        """

        try_sql("DROP FUNCTION IF EXISTS py_drop_t_a(INT);")
        test {
            sql """
                SELECT c
                FROM py_udtf_drop_tbl
                LATERAL VIEW py_drop_t_a(v) tmp AS c;
            """
            exception "Can not found function"
        }
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_drop_t_once(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_t_a(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_t_b(INT);")
    }
}
