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

suite("test_nereids_show_create_function") {
    sql "CREATE DATABASE IF NOT EXISTS show_create_function_db"
    sql """DROP FUNCTION if exists show_create_function_db.zzzyyyxxx_show_create_function_name(INT)"""
    sql """CREATE ALIAS FUNCTION show_create_function_db.zzzyyyxxx_show_create_function_name(INT) WITH PARAMETER(id)  
                AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"""
    sql """DROP GLOBAL FUNCTION if exists zzzyyyxxx_show_create_global_function_name(INT)"""
    sql """CREATE GLOBAL ALIAS FUNCTION zzzyyyxxx_show_create_global_function_name(INT) 
                WITH PARAMETER(id) AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"""

    checkNereidsExecute("use show_create_function_db; show create function zzzyyyxxx_show_create_function_name(INT);")
    checkNereidsExecute("show create global function zzzyyyxxx_show_create_global_function_name(INT);")
    checkNereidsExecute("show create function zzzyyyxxx_show_create_function_name(INT) from show_create_function_db")
    checkNereidsExecute("show create function zzzyyyxxx_show_create_function_name(INT) in show_create_function_db")

    def res = sql """use show_create_function_db; 
                                          show create function zzzyyyxxx_show_create_function_name(INT);"""
    assertTrue(res.size() == 1)
    assertEquals("zzzyyyxxx_show_create_function_name(int)", res.get(0).get(0))

    def res1 = sql """show create function zzzyyyxxx_show_create_function_name(INT) from show_create_function_db;"""
    assertTrue(res1.size() == 1)
    assertEquals("zzzyyyxxx_show_create_function_name(int)", res1.get(0).get(0))

    def res2 = sql """show create function zzzyyyxxx_show_create_function_name(INT) in show_create_function_db;"""
    assertTrue(res2.size() == 1)
    assertEquals("zzzyyyxxx_show_create_function_name(int)", res2.get(0).get(0))

    def res3 = sql """show create global function zzzyyyxxx_show_create_global_function_name(INT);"""
    assertTrue(res3.size() == 1)
    assertEquals("zzzyyyxxx_show_create_global_function_name(int)", res3.get(0).get(0))
}
