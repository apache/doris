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

    checkNereidsExecute("use ${dbName}; show builtin functions;")
    checkNereidsExecute("use ${dbName}; show builtin functions like 'ye%'")
    checkNereidsExecute("use ${dbName}; show full builtin functions;")
    checkNereidsExecute("use ${dbName}; show full builtin functions like 'ye%';")
    checkNereidsExecute("use ${dbName}; show functions;")
    checkNereidsExecute("use ${dbName}; show functions like '${functionName}%'")
    checkNereidsExecute("use ${dbName}; show full functions like '${functionName}%';")
    def res = sql """use ${dbName}; show builtin functions like '%yow%';"""
    assertTrue(res.size() == 1)
    def res1 = sql """use ${dbName}; show functions;"""
    assertTrue(res1.size() == 1)
    def res2 = sql """use ${dbName}; show functions like '${functionName}%';"""
    assertTrue(res2.size() == 1)
    // in nereids, each column of 'show full functions' is empty string, except Signature.
    def res3 = sql """use ${dbName}; show full functions like '${functionName}%';"""
    assertTrue(res3.size() == 1)
    assertEquals(res3.get(0).get(0), functionName)
    assertEquals(res3.get(0).get(1), "")
    assertEquals(res3.get(0).get(2), "")
    assertEquals(res3.get(0).get(3), "")
    assertEquals(res3.get(0).get(4), "")

    checkNereidsExecute("show builtin functions from ${dbName};")
    checkNereidsExecute("show builtin functions from ${dbName} like 'ye%';")
    checkNereidsExecute("show full builtin functions from ${dbName};")
    checkNereidsExecute("show full builtin functions from ${dbName} like 'ye%';")
    checkNereidsExecute("show functions from ${dbName};")
    checkNereidsExecute("show functions from ${dbName} like '${functionName}%';")
    checkNereidsExecute("show full functions from ${dbName};")
    checkNereidsExecute("show full functions from ${dbName} like '${functionName}%';")
    def res4 = sql """show builtin functions from ${dbName} like '%yow%';"""
    assertTrue(res4.size() == 1)
    def res5 = sql """show functions from ${dbName}"""
    assertTrue(res5.size() == 1)
    def res6 = sql """show functions from ${dbName} like '${functionName}%';"""
    assertTrue(res6.size() == 1)
    // in nereids, each column of 'show full functions' is empty string, except Signature.
    def res7 = sql """show full functions from ${dbName} like '${functionName}%';"""
    assertTrue(res7.size() == 1)
    assertEquals(res7.get(0).get(0), functionName)
    assertEquals(res7.get(0).get(1), "")
    assertEquals(res7.get(0).get(2), "")
    assertEquals(res7.get(0).get(3), "")
    assertEquals(res7.get(0).get(4), "")

    checkNereidsExecute("show builtin functions in ${dbName};")
    checkNereidsExecute("show builtin functions in ${dbName} like 'ye%';")
    checkNereidsExecute("show full builtin functions in ${dbName};")
    checkNereidsExecute("show full builtin functions in ${dbName} like 'ye%';")
    checkNereidsExecute("show functions in ${dbName};")
    checkNereidsExecute("show functions in ${dbName} like '${functionName}%';")
    checkNereidsExecute("show full functions in ${dbName};")
    checkNereidsExecute("show full functions in ${dbName} like '${functionName}%';")
    def res8 = sql """show builtin functions in ${dbName} like '%yow%';"""
    assertTrue(res8.size() == 1)
    def res9 = sql """show functions in ${dbName}"""
    assertTrue(res9.size() == 1)
    def res10 = sql """show functions in ${dbName} like '${functionName}%';"""
    assertTrue(res10.size() == 1)
    // in nereids, each column of 'show full functions' is empty string, except Signature.
    def res11 = sql """show full functions in ${dbName} like '${functionName}%';"""
    assertTrue(res11.size() == 1)
    assertEquals(res11.get(0).get(0), functionName)
    assertEquals(res11.get(0).get(1), "")
    assertEquals(res11.get(0).get(2), "")
    assertEquals(res11.get(0).get(3), "")
    assertEquals(res11.get(0).get(4), "")
}
