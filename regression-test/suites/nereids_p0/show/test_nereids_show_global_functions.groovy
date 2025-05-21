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

suite("test_nereids_show_global_functions") {
    String dbName = "show_functions_db"
    String functionName = "global_xyz"

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql """CREATE global ALIAS FUNCTION ${functionName}(INT) WITH PARAMETER(id)  AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"""

    checkNereidsExecute("use ${dbName}; show global functions;")
    checkNereidsExecute("use ${dbName}; show global full functions;")
    checkNereidsExecute("use ${dbName}; show global functions like 'global_%';")
    checkNereidsExecute("use ${dbName}; show global full functions like 'global_%';")

    def res = sql """use ${dbName}; show global functions like 'global_%';"""
    assertTrue(res.size() == 1)
    def res1 = sql """use ${dbName}; show global full functions like 'global_%';"""
    assertTrue(res1.size() == 1)
    // in nereids, each column of 'show full functions' is empty string, except Signature.
    def res3 = sql """use ${dbName}; show global full functions like '${functionName}%';"""
    assertTrue(res3.size() == 1)
    assertEquals(res3.get(0).get(0), functionName)
    assertEquals(res3.get(0).get(1), "")
    assertEquals(res3.get(0).get(2), "")
    assertEquals(res3.get(0).get(3), "")
    assertEquals(res3.get(0).get(4), "")
    sql """DROP GLOBAL FUNCTION  ${functionName}(INT)"""
}
