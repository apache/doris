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

suite("test_nereids_show_functions") {
    String dbName = "show_functions_db"
    String functionName = "zzzyyyxxx"

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql """DROP FUNCTION IF EXISTS ${dbName}.${functionName}(INT);"""
    sql """CREATE ALIAS FUNCTION ${dbName}.${functionName}(INT) WITH PARAMETER(id)  AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"""

    checkNereidsExecute("show builtin functions;")
    checkNereidsExecute("show builtin functions like 'ye%'")
    checkNereidsExecute("show full builtin functions;")
    checkNereidsExecute("show full builtin functions like 'ye%';")
    checkNereidsExecute("use ${dbName}; show functions;")
    checkNereidsExecute("use ${dbName}; show functions like '${functionName}%'")
    checkNereidsExecute("use ${dbName}; show full functions like '${functionName}%';")
    def res = sql """show builtin functions like '%yow%';"""
    assertTrue(res.size() == 1)
    def res1 = sql """use ${dbName}; show functions;"""
    assertTrue(res1.size() >= 1)
    def res2 = sql """use ${dbName}; show functions like '${functionName}%';"""
    assertTrue(res2.size() == 1)
    // in nereids, each column of 'show full functions' is empty string, except Signature.
    def res3 = sql """use ${dbName}; show full functions like '${functionName}%';"""
    assertTrue(res3.size() == 1)
    assertEquals(res3.get(0).get(0), "zzzyyyxxx(int)")
    assertEquals(res3.get(0).get(1), "varchar(65533)")
    assertEquals(res3.get(0).get(2), "ALIAS/JAVA_UDF")
    assertEquals(res3.get(0).get(3), "")
    assertTrue(res3[0][4].contains("ID="))
    assertTrue(res3[0][4].contains("NULLABLE_MODE="))
    assertTrue(res3[0][4].contains("ALIAS_OF="))

    checkNereidsExecute("show builtin functions;")
    checkNereidsExecute("show builtin functions like 'ye%';")
    checkNereidsExecute("show full builtin functions;")
    checkNereidsExecute("show full builtin functions like 'ye%';")
    checkNereidsExecute("show functions from ${dbName};")
    checkNereidsExecute("show functions from ${dbName} like '${functionName}%';")
    checkNereidsExecute("show full functions from ${dbName};")
    checkNereidsExecute("show full functions from ${dbName} like '${functionName}%';")
    def res4 = sql """show builtin functions like '%yow%';"""
    assertTrue(res4.size() == 1)
    def res5 = sql """show functions from ${dbName}"""
    assertTrue(res5.size() >= 1)
    def res6 = sql """show functions from ${dbName} like '${functionName}%';"""
    assertTrue(res6.size() == 1)
    // in nereids, each column of 'show full functions' is empty string, except Signature.
    def res7 = sql """show full functions like '${functionName}%';"""
    assertTrue(res7.size() == 1)
    assertEquals(res7.get(0).get(0), "zzzyyyxxx(int)")
    assertEquals(res7.get(0).get(1), "varchar(65533)")
    assertEquals(res7.get(0).get(2), "ALIAS/JAVA_UDF")
    assertEquals(res7.get(0).get(3), "")
    assertTrue(res7[0][4].contains("ID="))
    assertTrue(res7[0][4].contains("NULLABLE_MODE="))
    assertTrue(res7[0][4].contains("ALIAS_OF="))

    checkNereidsExecute("show builtin functions;")
    checkNereidsExecute("show builtin functions like 'ye%';")
    checkNereidsExecute("show full builtin functions;")
    checkNereidsExecute("show full builtin functions like 'ye%';")
    checkNereidsExecute("show functions in ${dbName};")
    checkNereidsExecute("show functions in ${dbName} like '${functionName}%';")
    checkNereidsExecute("show full functions in ${dbName};")
    checkNereidsExecute("show full functions in ${dbName} like '${functionName}%';")
    def res8 = sql """show builtin functions like '%yow%';"""
    assertTrue(res8.size() == 1)
    def res9 = sql """show functions in ${dbName}"""
    assertTrue(res9.size() >= 1)
    def res10 = sql """show functions in ${dbName} like '${functionName}%';"""
    assertTrue(res10.size() == 1)
    // in nereids, each column of 'show full functions' is empty string, except Signature.
    def res11 = sql """show full functions in ${dbName} like '${functionName}%';"""
    assertTrue(res11.size() == 1)
    assertEquals(res11.get(0).get(0), "zzzyyyxxx(int)")
    assertEquals(res11.get(0).get(1), "varchar(65533)")
    assertEquals(res11.get(0).get(2), "ALIAS/JAVA_UDF")
    assertEquals(res11.get(0).get(3), "")
    assertTrue(res11[0][4].contains("ID="))
    assertTrue(res11[0][4].contains("NULLABLE_MODE="))
    assertTrue(res11[0][4].contains("ALIAS_OF="))

    def runtime_version = "3.8.10"
    def suitePath = context.file.parent + "/../.."

    sql """ DROP FUNCTION IF EXISTS py_add(int, int) """
    sql """ CREATE FUNCTION py_add(INT, INT)
            RETURNS INT
            PROPERTIES (
                "type" = "PYTHON_UDF",
                "symbol" = "evaluate",
                "runtime_version" = "${runtime_version}"
            )
            AS \$\$
            def evaluate(a, b):
                return a + b
            \$\$;
    """
    def pyinline_res = sql """show full functions like 'py_add';"""
    assertTrue(pyinline_res.size() == 1)
    assertEquals("py_add(int, int)", pyinline_res[0][0])
    assertEquals("int", pyinline_res[0][1])
    assertEquals("SCALAR/PYTHON_UDF", pyinline_res[0][2])
    assertEquals("", pyinline_res[0][3])
    assertTrue(pyinline_res[0][4].contains("ID="))
    assertTrue(pyinline_res[0][4].contains("NULLABLE_MODE=ALWAYS_NULLABLE"))
    assertTrue(pyinline_res[0][4].contains("RUNTIME_VERSION=${runtime_version}"))
    assertTrue(pyinline_res[0][4].contains("SYMBOL=evaluate"))
    assertTrue(pyinline_res[0][4].contains("INLINE_CODE="))

    sql """ DROP FUNCTION IF EXISTS python_udf_int_test(int) """
    sql """ CREATE FUNCTION python_udf_int_test(int) RETURNS int PROPERTIES (
            "file"="file://${suitePath}/pythonudf_p0/udf_scripts/pyudf.zip",
            "symbol"="int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
    """
    def pyudf_res = sql """show full functions like 'python_udf_int_test';"""
    assertTrue(pyudf_res.size() == 1)
    assertEquals("python_udf_int_test(int)", pyudf_res[0][0])
    assertEquals("int", pyudf_res[0][1])
    assertEquals("SCALAR/PYTHON_UDF", pyudf_res[0][2])
    assertEquals("", pyudf_res[0][3])
    assertTrue(pyudf_res[0][4].contains("ID="))
    assertTrue(pyudf_res[0][4].contains("CHECKSUM="))
    assertTrue(pyudf_res[0][4].contains("OBJECT_FILE="))
    assertTrue(pyudf_res[0][4].contains("pythonudf_p0/udf_scripts/pyudf.zip"))
    assertTrue(pyudf_res[0][4].contains("NULLABLE_MODE=ALWAYS_NULLABLE"))
    assertTrue(pyudf_res[0][4].contains("RUNTIME_VERSION=${runtime_version}"))
    assertTrue(pyudf_res[0][4].contains("SYMBOL=int_test.evaluate"))

    sql """ DROP FUNCTION IF EXISTS python_udaf_sum_int(int) """
    sql """ CREATE AGGREGATE FUNCTION python_udaf_sum_int(int) RETURNS bigint PROPERTIES (
            "file"="file://${suitePath}/pythonudf_p0/udf_scripts/pyudf.zip",
            "symbol"="sum_int.SumInt",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
    """
    def pyudaf_res = sql """show full functions like 'python_udaf_sum_int';"""
    assertTrue(pyudaf_res.size() == 1)
    assertEquals("python_udaf_sum_int(int)", pyudaf_res[0][0])
    assertEquals("bigint", pyudaf_res[0][1])
    assertEquals("AGGREGATE/PYTHON_UDF", pyudaf_res[0][2])
    assertTrue(pyudaf_res[0][4].contains("ID="))
    assertTrue(pyudaf_res[0][4].contains("CHECKSUM="))
    assertTrue(pyudaf_res[0][4].contains("OBJECT_FILE="))
    assertTrue(pyudaf_res[0][4].contains("pythonudf_p0/udf_scripts/pyudf.zip"))
    assertTrue(pyudaf_res[0][4].contains("NULLABLE_MODE=ALWAYS_NULLABLE"))
    assertTrue(pyudaf_res[0][4].contains("RUNTIME_VERSION=${runtime_version}"))
    assertTrue(pyudaf_res[0][4].contains("SYMBOL=sum_int.SumInt"))

    sql """ DROP FUNCTION IF EXISTS py_split_string_module(STRING) """
    sql """CREATE TABLES FUNCTION py_split_string_module(STRING)
        RETURNS ARRAY<STRING> PROPERTIES (
            "file" = "file://${suitePath}/pythonudtf_p0/udtf_scripts/pyudtf.zip",
            "symbol" = "pyudtf_module.basic_udtf.split_string_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
    """
    def pyudtf_res = sql """show full functions like 'py_split_string_module';"""
    assertTrue(pyudtf_res.size() == 1)
    assertEquals("py_split_string_module(text)", pyudtf_res[0][0])
    assertEquals("text", pyudtf_res[0][1])
    assertEquals("UDTF/PYTHON_UDF", pyudtf_res[0][2])
    assertEquals("", pyudtf_res[0][3])
    assertTrue(pyudtf_res[0][4].contains("ID="))
    assertTrue(pyudtf_res[0][4].contains("CHECKSUM="))
    assertTrue(pyudtf_res[0][4].contains("OBJECT_FILE="))
    assertTrue(pyudtf_res[0][4].contains("pythonudtf_p0/udtf_scripts/pyudtf.zip"))
    assertTrue(pyudtf_res[0][4].contains("NULLABLE_MODE=ALWAYS_NULLABLE"))
    assertTrue(pyudtf_res[0][4].contains("RUNTIME_VERSION=${runtime_version}"))
    assertTrue(pyudtf_res[0][4].contains("SYMBOL=pyudtf_module.basic_udtf.split_string_udtf"))

}
