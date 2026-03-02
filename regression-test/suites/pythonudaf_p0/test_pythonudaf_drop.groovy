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

suite('test_pythonudaf_drop') {
    def runtime_version = '3.8.10'
    def zipA = """${context.file.parent}/udaf_scripts/python_udaf_drop_a/python_udaf_drop_test.zip"""
    def zipB = """${context.file.parent}/udaf_scripts/python_udaf_drop_b/python_udaf_drop_test.zip"""

    scp_udf_file_to_all_be(zipA)
    scp_udf_file_to_all_be(zipB)

    sql '''DROP TABLE IF EXISTS py_udaf_drop_tbl'''
    sql '''
        CREATE TABLE py_udaf_drop_tbl (
            v INT
        ) ENGINE=OLAP
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    '''
    sql '''INSERT INTO py_udaf_drop_tbl VALUES (1), (2), (3);'''

    try {
        // Case 1: simple drop should make subsequent call fail
        sql '''DROP FUNCTION IF EXISTS py_drop_sum_once(INT)'''
        sql """
            CREATE AGGREGATE FUNCTION py_drop_sum_once(INT) RETURNS BIGINT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "drop_udaf.SumAgg",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_py_udaf_drop_1 '''SELECT py_drop_sum_once(v) FROM py_udaf_drop_tbl;'''
        try_sql('DROP FUNCTION IF EXISTS py_drop_sum_once(INT);')
        test {
            sql '''SELECT py_drop_sum_once(v) FROM py_udaf_drop_tbl;'''
            exception 'Can not found function'
        }

        // Case 2: same module name, different file paths
        sql '''DROP FUNCTION IF EXISTS py_drop_sum_a(INT)'''
        sql '''DROP FUNCTION IF EXISTS py_drop_sum_b(INT)'''
        sql """
            CREATE AGGREGATE FUNCTION py_drop_sum_a(INT) RETURNS BIGINT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "drop_udaf.SumAgg",
                "runtime_version" = "${runtime_version}"
            )
        """
        sql """
            CREATE AGGREGATE FUNCTION py_drop_sum_b(INT) RETURNS BIGINT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipB}",
                "symbol" = "drop_udaf.SumAgg",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_py_udaf_drop_2 '''SELECT py_drop_sum_a(v), py_drop_sum_b(v) FROM py_udaf_drop_tbl;'''

        try_sql('DROP FUNCTION IF EXISTS py_drop_sum_b(INT);')
        test {
            sql '''SELECT py_drop_sum_b(v) FROM py_udaf_drop_tbl;'''
            exception 'Can not found function'
        }

        qt_py_udaf_drop_3 '''SELECT py_drop_sum_a(v) FROM py_udaf_drop_tbl;'''

        try_sql('DROP FUNCTION IF EXISTS py_drop_sum_a(INT);')
        test {
            sql '''SELECT py_drop_sum_a(v) FROM py_udaf_drop_tbl;'''
            exception 'Can not found function'
        }
    } finally {
        try_sql('DROP FUNCTION IF EXISTS py_drop_sum_once(INT);')
        try_sql('DROP FUNCTION IF EXISTS py_drop_sum_a(INT);')
        try_sql('DROP FUNCTION IF EXISTS py_drop_sum_b(INT);')
    }
}
