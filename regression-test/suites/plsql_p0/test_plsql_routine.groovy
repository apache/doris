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
suite("test_plsql_routine") {
    def dbName = "plsql_routine"
    sql "drop database if exists ${dbName}"
    sql """DROP PROCEDURE test_plsql_routine1"""
    sql """DROP PROC test_plsql_routine2"""
    sql """DROP PROC test_plsql_routine3"""
    sql """DROP PROC test_plsql_routine4"""
    sql """DROP PROC test_plsql_routine5"""

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"

    def procedure_body = "BEGIN DECLARE a int = 1; print a; END;"
    def select_routines_count = """select count(*) from information_schema.routines where routine_schema=\"${dbName}\";"""
    def select_routines_fixed_value_column = """select SPECIFIC_NAME,ROUTINE_CATALOG,ROUTINE_SCHEMA,ROUTINE_NAME,ROUTINE_TYPE
            ,DTD_IDENTIFIER,ROUTINE_BODY,ROUTINE_DEFINITION,EXTERNAL_NAME,EXTERNAL_NAME,EXTERNAL_LANGUAGE,PARAMETER_STYLE
            ,IS_DETERMINISTIC,SQL_DATA_ACCESS,SQL_PATH,SECURITY_TYPE,SQL_MODE,ROUTINE_COMMENT,DEFINER,CHARACTER_SET_CLIENT
            ,COLLATION_CONNECTION,DATABASE_COLLATION from information_schema.routines where routine_schema=\"${dbName}\" 
            order by SPECIFIC_NAME;"""

    sql """ CREATE OR REPLACE PROCEDURE test_plsql_routine1() ${procedure_body} """
    sql """ CREATE OR REPLACE PROCEDURE test_plsql_routine2() ${procedure_body} """
    sql """ CREATE OR REPLACE PROCEDURE test_plsql_routine3() ${procedure_body} """

    sql """select * from information_schema.routines where routine_schema=\"${dbName}\";"""
    qt_select """${select_routines_count}"""
    qt_select """${select_routines_fixed_value_column}"""

    sql """ CREATE OR REPLACE PROCEDURE test_plsql_routine4() ${procedure_body} """
    sql """ CREATE OR REPLACE PROCEDURE test_plsql_routine5() ${procedure_body} """

    qt_select """${select_routines_count}"""
    qt_select """${select_routines_fixed_value_column}"""

    sql """DROP PROCEDURE test_plsql_routine1"""

    qt_select """${select_routines_count}"""
    qt_select """${select_routines_fixed_value_column}"""

    sql """ CREATE OR REPLACE PROCEDURE test_plsql_routine1() ${procedure_body} """

    qt_select """${select_routines_count}"""
    qt_select """${select_routines_fixed_value_column}"""
}
