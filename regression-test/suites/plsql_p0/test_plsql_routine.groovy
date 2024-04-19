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
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"
    def tableName = "plsql_tbl_4"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """

    sql """
        CREATE OR REPLACE PROCEDURE routine_insert(IN id int, IN name STRING)
        BEGIN
            INSERT INTO ${tableName} VALUES(id, name);
        END;
        """
    sql """call routine_insert(111, "plsql111")"""
    sql """call routine_insert(222, "plsql222")"""
    sql """call routine_insert(333, "plsql333")"""
    sql """call routine_insert(111, "plsql333")"""
    qt_select "select sum(id), count(1) from ${tableName}"

    sql """
        CREATE OR REPLACE PROCEDURE routine_cursor_select(IN id_arg INT, IN name_arg STRING) 
        BEGIN
        DECLARE a INT;
        DECLARE b, c STRING;

        DECLARE cur1 CURSOR FOR select * from ${tableName} where id=id_arg limit 5;
        OPEN cur1;
        read_loop: LOOP
            FETCH cur1 INTO a, b;
            IF(SQLCODE != 0) THEN
                LEAVE read_loop;
            END IF;
            print a, b;
        END LOOP;

        CLOSE cur1;

        END;
        """

    qt_select "select count(*) from information_schema.routines where routine_schema=\"${dbName}\";"
    sql """select * from information_schema.routines;"""
    sql """call routine_cursor_select(111, "plsql111")"""
    sql """call routine_cursor_select(111, "plsql333")"""
    sql """SHOW CREATE PROCEDURE routine_cursor_select;"""
    // TODO call show command before drop
    sql """DROP PROCEDURE routine_cursor_select"""
    sql """DROP PROC routine_insert"""
    sql "DROP DATABASE ${dbName}"
    // TODO call show command after drop
}
