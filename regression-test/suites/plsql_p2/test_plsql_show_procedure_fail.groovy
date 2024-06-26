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

// Add PL-SQL regression test notice:
// 1. JDBC does not support the execution of stored procedures that return results. You can only Into the execution
// results into a variable or write them into a table, because when multiple result sets are returned, JDBC needs
// to use the prepareCall statement to execute, otherwise the Statemnt of the returned result executes Finalize.
// Send EOF Packet will report an error;
// 2. The format of the result returned by Doris Statement is xxxx\n, xxxx\n, 2 rows affected (0.03 sec).
// PL-SQL uses Print to print variable values in an unformatted format, and JDBC cannot easily obtain them. Real results.
suite("test_plsql_show_procedure_fail") {
    try {
         sql """SET enable_fallback_to_original_planner=false;"""
         sql """SHOW PROCEDURE STATUS where Db="Test" and db="Test1";"""
         fail();
    } catch (Throwable t) {
         log.info("{}", t.toString())
         if (t instanceof java.sql.SQLException) {
             String result = t.toString()
             String expected = "java.sql.SQLException: errCode = 2, detailMessage = WhereClause can contain one predicate for one column.";
             boolean check = result.equals(expected);
             assertTrue(check)
         } else {
		fail()
         }
    }
    try {
         sql """SET enable_fallback_to_original_planner=false;"""
         sql """SHOW PROCEDURE STATUS where Name="Test" and name="Test1";"""
         fail();
    } catch (Throwable t) {
         log.info("{}", t.toString())
         if (t instanceof java.sql.SQLException) {
             String result = t.toString()
             String expected = "java.sql.SQLException: errCode = 2, detailMessage = WhereClause can contain one predicate for one column.";
             boolean check = result.equals(expected);
             assertTrue(check)
         } else {
		fail()
         }
    }
    try {
         sql """SET enable_fallback_to_original_planner=false;"""
         sql """SHOW PROCEDURE STATUS where Db="Test" or name="Test1";"""
         fail();
    } catch (Throwable t) {
         log.info("{}", t.toString())
         if (t instanceof java.sql.SQLException) {
             String result = t.toString()
             String expected = "java.sql.SQLException: errCode = 2, detailMessage = only support AND conjunction, does not support OR.";
             boolean check = result.equals(expected);
             assertTrue(check)
         } else {
		fail()
         }
    }
    try {
         sql """SET enable_fallback_to_original_planner=false;"""
         sql """SHOW PROCEDURE STATUS where Db="Test" and name="Test1" and code = "test";"""
         fail();
    } catch (Throwable t) {
         log.info("{}", t.toString())
         if (t instanceof java.sql.SQLException) {
             String result = t.toString()
             String expected = "java.sql.SQLException: errCode = 2, detailMessage = Only supports filter procedurename, name,procedurename with equalTo or LIKE";
             boolean check = result.equals(expected);
             assertTrue(check)
         } else {
		fail()
         }
    }
    try {
         sql """SET enable_fallback_to_original_planner=false;"""
         sql """SHOW PROCEDURE STATUS where Db="Test" and name="Test1" and procedurename = "test";"""
         fail();
    } catch (Throwable t) {
         log.info("{}", t.toString())
         if (t instanceof java.sql.SQLException) {
             String result = t.toString()
             String expected = "java.sql.SQLException: errCode = 2, detailMessage = Only supports filter Name/ProcedureName only 1 time in where clause";
             boolean check = result.equals(expected);
             assertTrue(check)
         } else {
		fail()
         }
    }           
}
