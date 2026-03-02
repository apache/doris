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

suite("test_pythonudf_drop") {
    def runtime_version = "3.8.10"
    def zipA = """${context.file.parent}/udf_scripts/python_udf_drop_a/python_udf_drop_test.zip"""
    def zipB = """${context.file.parent}/udf_scripts/python_udf_drop_b/python_udf_drop_test.zip"""

    scp_udf_file_to_all_be(zipA)
    scp_udf_file_to_all_be(zipB)

    sql """DROP TABLE IF EXISTS py_udf_drop_tbl"""
    sql """
        CREATE TABLE py_udf_drop_tbl (
            id INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql """INSERT INTO py_udf_drop_tbl VALUES (1), (2), (3);"""

    try {
        // Case 1: simple drop should make subsequent call fail
        sql """DROP FUNCTION IF EXISTS py_drop_once(INT)"""
        sql """
            CREATE FUNCTION py_drop_once(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "drop_udf.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_py_udf_drop_1 """SELECT py_drop_once(10);"""
        try_sql("DROP FUNCTION IF EXISTS py_drop_once(INT);")
        test {
            sql """SELECT py_drop_once(10);"""
            exception "Can not found function"
        }

        // Case 2: same module name, different file paths
        sql """DROP FUNCTION IF EXISTS py_drop_a(INT)"""
        sql """DROP FUNCTION IF EXISTS py_drop_b(INT)"""
        sql """
            CREATE FUNCTION py_drop_a(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "drop_udf.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """
        sql """
            CREATE FUNCTION py_drop_b(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "drop_udf.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_py_udf_drop_2 """SELECT py_drop_a(5), py_drop_b(5);"""

        try_sql("DROP FUNCTION IF EXISTS py_drop_b(INT);")
        test {
            sql """SELECT py_drop_b(5);"""
            exception "Can not found function"
        }

        qt_py_udf_drop_3 """SELECT py_drop_a(7);"""

        try_sql("DROP FUNCTION IF EXISTS py_drop_a(INT);")
        test {
            sql """SELECT py_drop_a(1);"""
            exception "Can not found function"
        }
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_drop_once(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_a(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_b(INT);")
    }
}
