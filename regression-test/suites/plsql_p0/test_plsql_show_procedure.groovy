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
suite("test_plsql_show_procedure") {
    def dbName = "plsql_show_procedure_db"
    sql "drop database if exists ${dbName}"
    sql """DROP PROC test_plsql_show_proc1"""
    sql """DROP PROC test_plsql_show_proc2"""
    sql """DROP PROC test_plsql_show_proc3"""
    sql """DROP PROC test_plsql_show_proc4"""
    sql """DROP PROC test_plsql_show_proc5"""

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"
    
    def procedure_body = "BEGIN DECLARE a int = 1; print a; END;"
    sql """ CREATE OR REPLACE PROCEDURE test_plsql_show_proc1() ${procedure_body} """
    test {
             sql """SHOW PROCEDURE STATUS where Db="${dbName}";"""
             rowNum 1
             check { result, exception, startTime, endTime ->
             assertEquals("TEST_PLSQL_SHOW_PROC1", result[0][0]);
             }
        }

    sql """ CREATE OR REPLACE PROCEDURE test_plsql_show_proc2() ${procedure_body} """
    sql """ CREATE OR REPLACE PROCEDURE test_plsql_show_proc3() ${procedure_body} """

    test {
             sql """SHOW PROCEDURE STATUS where Db="${dbName}";"""
             rowNum 3
             check { result, exception, startTime, endTime ->
                result = sortRows(result);
             	assertEquals("TEST_PLSQL_SHOW_PROC1", result[0][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC2", result[1][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC3", result[2][0]);
             }
        }


    sql """ CREATE OR REPLACE PROCEDURE test_plsql_show_proc4() ${procedure_body} """
    sql """ CREATE OR REPLACE PROCEDURE test_plsql_show_proc5() ${procedure_body} """

    test {
             sql """SHOW PROCEDURE STATUS where Db="${dbName}";"""
             rowNum 5
             check { result, exception, startTime, endTime ->
                result = sortRows(result);
             	assertEquals("TEST_PLSQL_SHOW_PROC1", result[0][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC2", result[1][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC3", result[2][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC4", result[3][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC5", result[4][0]);
             }
        }

    sql """DROP PROCEDURE test_plsql_show_proc1"""

    test {
             sql """SHOW PROCEDURE STATUS where Db="${dbName}";"""
             rowNum 4
             check { result, exception, startTime, endTime ->
                result = sortRows(result);
             	assertEquals("TEST_PLSQL_SHOW_PROC2", result[0][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC3", result[1][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC4", result[2][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC5", result[3][0]);
             }
        }

    sql """ CREATE OR REPLACE PROCEDURE test_plsql_show_proc1() ${procedure_body} """

    test {
             sql """SHOW PROCEDURE STATUS where Db="${dbName}";"""
             rowNum 5
             check { result, exception, startTime, endTime ->
                result = sortRows(result);
             	assertEquals("TEST_PLSQL_SHOW_PROC1", result[0][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC2", result[1][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC3", result[2][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC4", result[3][0]);
             	assertEquals("TEST_PLSQL_SHOW_PROC5", result[4][0]);
             }
        }

    sql """SHOW PROCEDURE STATUS;"""
    sql """SHOW PROCEDURE STATUS where db="${dbName}";"""
    sql """SHOW PROCEDURE STATUS where Db="${dbName}" and name = "test_plsql_show_proc1";"""
    sql """SHOW PROCEDURE STATUS where Db="${dbName}" and Name LIKE "test_plsql_show_proc1";"""
    sql """SHOW PROCEDURE STATUS where procedureName="test_plsql_show_proc1";"""
  
    test {
        sql """SHOW PROCEDURE STATUS where procedureName="not_exist_procedure";"""
        check { result, ex, startTime, endTime -> 
             assertTrue(result.isEmpty());
        }
    }

    test {
        sql """SHOW CREATE PROCEDURE not_exist_procedure;"""
        check { result, ex, startTime, endTime ->
             assertTrue(result.isEmpty());
        }
    }



    sql """DROP PROC test_plsql_show_proc1"""
    sql """DROP PROC test_plsql_show_proc2"""
    sql """DROP PROC test_plsql_show_proc3"""
    sql """DROP PROC test_plsql_show_proc4"""
    sql """DROP PROC test_plsql_show_proc5"""
}
