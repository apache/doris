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

suite("test_python_raise_error_propagation") {
    // Keep using the existing type-wide archives under regression-test/suites.
    // This avoids introducing extra zip names and preserves the same loader/cache path shape as p0.
    def suitePath = context.file.parent + "/.."
    def udfPath = """${suitePath}/pythonudf_p0/udf_scripts/pyudf.zip"""
    def udafPath = """${suitePath}/pythonudaf_p0/udaf_scripts/pyudaf.zip"""
    def udtfPath = """${suitePath}/pythonudtf_p0/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(udfPath)
    scp_udf_file_to_all_be(udafPath)
    scp_udf_file_to_all_be(udtfPath)
    def runtime_version = getPythonUdfRuntimeVersion()
    log.info("Python UDF zip path: ${udfPath}".toString())
    log.info("Python UDAF zip path: ${udafPath}".toString())
    log.info("Python UDTF zip path: ${udtfPath}".toString())

    try {
        sql """ DROP TABLE IF EXISTS python_raise_error_test; """
        sql """
        CREATE TABLE python_raise_error_test (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO python_raise_error_test VALUES
        (1, 1),
        (2, 2);
        """

        sql """ DROP FUNCTION IF EXISTS py_inline_raise_udf(INT); """
        sql """
        CREATE FUNCTION py_inline_raise_udf(INT)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        ) AS \$\$
def evaluate(x):
    raise TypeError("inline_udf_error_42")
\$\$;
        """

        test {
            sql """ SELECT py_inline_raise_udf(1); """
            exception "inline_udf_error_42"
        }

        sql """ DROP FUNCTION IF EXISTS py_module_raise_udf(INT); """
        sql """
        CREATE FUNCTION py_module_raise_udf(INT)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${udfPath}",
            "symbol" = "udf_errors.raise_in_module",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """

        test {
            sql """ SELECT py_module_raise_udf(1); """
            exception "module_udf_error_42"
        }

        sql """ DROP FUNCTION IF EXISTS py_inline_raise_udaf(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_inline_raise_udaf(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "InlineFinishErrorUDAF",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        ) AS \$\$
class InlineFinishErrorUDAF:
    def __init__(self):
        self.count = 0

    @property
    def aggregate_state(self):
        return self.count

    def accumulate(self, value):
        if value is not None:
            self.count += 1

    def merge(self, other_state):
        self.count += other_state

    def finish(self):
        raise TypeError("inline_udaf_error_42")
\$\$;
        """

        test {
            sql """ SELECT py_inline_raise_udaf(val) FROM python_raise_error_test; """
            exception "inline_udaf_error_42"
        }

        sql """ DROP FUNCTION IF EXISTS py_module_raise_udaf(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_module_raise_udaf(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${udafPath}",
            "symbol" = "udaf_errors.ModuleFinishErrorUDAF",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """

        test {
            sql """ SELECT py_module_raise_udaf(val) FROM python_raise_error_test; """
            exception "module_udaf_error_42"
        }

        sql """ DROP FUNCTION IF EXISTS py_inline_raise_udtf(INT); """
        sql """
        CREATE TABLES FUNCTION py_inline_raise_udtf(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "inline_raise_udtf",
            "runtime_version" = "${runtime_version}"
        ) AS \$\$
def inline_raise_udtf(x):
    if False:
        yield x
    raise TypeError("inline_udtf_error_42")
\$\$;
        """

        test {
            sql """
            SELECT tmp.col
            FROM python_raise_error_test
            LATERAL VIEW py_inline_raise_udtf(val) tmp AS col;
            """
            exception "inline_udtf_error_42"
        }

        sql """ DROP FUNCTION IF EXISTS py_module_raise_udtf(INT); """
        sql """
        CREATE TABLES FUNCTION py_module_raise_udtf(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${udtfPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.raise_in_module_udtf",
            "runtime_version" = "${runtime_version}"
        );
        """

        test {
            sql """
            SELECT tmp.col
            FROM python_raise_error_test
            LATERAL VIEW py_module_raise_udtf(val) tmp AS col;
            """
            exception "module_udtf_error_42"
        }
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_inline_raise_udf(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_module_raise_udf(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_inline_raise_udaf(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_module_raise_udaf(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_inline_raise_udtf(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_module_raise_udtf(INT);")
        try_sql("DROP TABLE IF EXISTS python_raise_error_test;")
    }
}
