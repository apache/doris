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

suite("test_pythonudf_drop", "nonConcurrent") {
    def runtime_version = getPythonUdfRuntimeVersion()
    def zipA = """${context.file.parent}/udf_scripts/python_udf_drop_a/python_udf_drop_test.zip"""
    def zipB = """${context.file.parent}/udf_scripts/python_udf_drop_b/python_udf_drop_test.zip"""
    def localDorisHome = System.getenv("DORIS_HOME")
    def localUdfRoot = localDorisHome != null ? "${localDorisHome}/lib/udf" : "/tmp"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    def execOnBackend = { be_ip, localCmd, remoteCmd ->
        if (be_ip == "127.0.0.1" || be_ip == "localhost") {
            cmd(localCmd)
        } else {
            sshExec("root", be_ip, remoteCmd, false)
        }
    }

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

        // Case 3: kill Python servers between two queries, next client handshake should recover
        sql """DROP FUNCTION IF EXISTS py_drop_reconnect(INT)"""
        sql """
            CREATE FUNCTION py_drop_reconnect(INT) RETURNS INT PROPERTIES (
                "type" = "PYTHON_UDF",
                "file" = "file://${zipA}",
                "symbol" = "drop_udf.evaluate",
                "runtime_version" = "${runtime_version}"
            )
        """

        qt_py_udf_drop_4 """SELECT py_drop_reconnect(31);"""

        backendId_to_backendIP.values().each { be_ip ->
            execOnBackend(
                be_ip,
                "pkill -f 'python_server.py grpc+unix:///tmp/doris_python_udf' || true",
                "pkill -f 'python_server.py grpc+unix:///tmp/doris_python_udf' || true")
        }

        qt_py_udf_drop_5 """SELECT py_drop_reconnect(32);"""
        try_sql("DROP FUNCTION IF EXISTS py_drop_reconnect(INT);")

        // Case 4: recreating the same signature must use the new inline function body.
        sql """DROP FUNCTION IF EXISTS py_drop_recreate(INT)"""
        sql """
            CREATE FUNCTION py_drop_recreate(INT)
            RETURNS INT
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "symbol" = "evaluate",
                "runtime_version" = "${runtime_version}",
                "always_nullable" = "true"
            )
            AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 1
\$\$
        """
        def recreateOldResult = sql """SELECT py_drop_recreate(10);"""
        assert recreateOldResult[0][0] == 11

        sql """DROP FUNCTION IF EXISTS py_drop_recreate(INT)"""
        sql """
            CREATE FUNCTION py_drop_recreate(INT)
            RETURNS INT
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "symbol" = "evaluate",
                "runtime_version" = "${runtime_version}",
                "always_nullable" = "true"
            )
            AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 999
\$\$
        """
        def recreateNewResult = sql """SELECT py_drop_recreate(10);"""
        assert recreateNewResult[0][0] == 1009
        sql """DROP FUNCTION IF EXISTS py_drop_recreate(INT)"""

        // Case 5: dropping a database must also clear Nereids UDF registry.
        // SHOW FUNCTIONS reads catalog metadata, while SELECT resolves from FunctionRegistry.
        // Without registry cleanup, SELECT could still bind the stale x + 1 inline UDF
        // after the database had been dropped and recreated.
        def originalDb = sql("SELECT DATABASE()")[0][0]
        def registryDb = "${originalDb}_registry_cleanup"
        try {
            sql """DROP DATABASE IF EXISTS ${registryDb} FORCE"""
            sql """CREATE DATABASE ${registryDb}"""
            sql """USE ${registryDb}"""
            sql """
                CREATE FUNCTION py_drop_db_registry(INT)
                RETURNS INT
                PROPERTIES (
                    "type" = "PYTHON_UDF",
                    "symbol" = "evaluate",
                    "runtime_version" = "${runtime_version}",
                    "always_nullable" = "true"
                )
                AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 1
\$\$
            """
            def oldResult = sql """SELECT py_drop_db_registry(10);"""
            assert oldResult[0][0] == 11

            sql """DROP DATABASE ${registryDb} FORCE"""
            sql """CREATE DATABASE ${registryDb}"""
            sql """USE ${registryDb}"""
            def functions = sql """SHOW FUNCTIONS LIKE 'py_drop_db_registry'"""
            assert functions.isEmpty()
            test {
                sql """SELECT py_drop_db_registry(10);"""
                exception "Can not found function"
            }

            sql """
                CREATE FUNCTION py_drop_db_registry(INT)
                RETURNS INT
                PROPERTIES (
                    "type" = "PYTHON_UDF",
                    "symbol" = "evaluate",
                    "runtime_version" = "${runtime_version}",
                    "always_nullable" = "true"
                )
                AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 999
\$\$
            """
            def rebuiltResult = sql """SELECT py_drop_db_registry(10);"""
            assert rebuiltResult[0][0] == 1009
        } finally {
            sql """USE ${originalDb}"""
            try_sql("DROP DATABASE IF EXISTS ${registryDb} FORCE")
        }
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_drop_once(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_a(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_b(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_reconnect(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_drop_recreate(INT);")
    }
}
