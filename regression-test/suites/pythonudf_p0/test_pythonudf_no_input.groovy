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

suite("test_pythonudf_no_input") {
    def runtime_version = getPythonUdfRuntimeVersion()
    def table_name = "test_pythonudf_no_input_tbl"

    try {
        sql """ DROP FUNCTION IF EXISTS py_const_no_input(); """
        sql """ DROP TABLE IF EXISTS ${table_name}; """

        sql """
        CREATE FUNCTION py_const_no_input()
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate():
    return 7
\$\$;
        """

        assert sql(""" SELECT py_const_no_input(); """)[0][0] == 7

        sql """
        CREATE TABLE ${table_name} (
            id INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO ${table_name} VALUES (1), (2), (3); """

        def rows = sql("""
            SELECT id, py_const_no_input() AS v
            FROM ${table_name}
            ORDER BY id
        """)

        assert rows.size() == 3 : "Expected 3 rows, got ${rows.size()}"
        assert rows.collect { it[0] as int } == [1, 2, 3]
        assert rows.every { (it[1] as int) == 7 }
    } finally {
        try_sql(""" DROP FUNCTION IF EXISTS py_const_no_input(); """)
        try_sql(""" DROP TABLE IF EXISTS ${table_name}; """)
    }
}
