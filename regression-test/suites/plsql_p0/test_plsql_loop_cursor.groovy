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

suite("test_plsql_loop_cursor") {
    def tableName = "plsql_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """

    sql """
        CREATE OR REPLACE PROCEDURE procedure_insert(IN id int, IN name STRING)
        BEGIN
            INSERT INTO ${tableName} VALUES(id, name);
        END;
        """
    sql """call procedure_insert(111, "plsql111")"""
    sql """call procedure_insert(222, "plsql222")"""
    sql """call procedure_insert(333, "plsql333")"""
    sql """call procedure_insert(111, "plsql333")"""
    qt_select "select sum(id), count(1) from ${tableName}"

    sql """
        CREATE OR REPLACE PROCEDURE procedure_cursor_select(IN id_arg INT, IN name_arg STRING) 
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

    // TODO support print
    sql """call procedure_cursor_select(111, "plsql111")"""
    sql """call procedure_cursor_select(111, "plsql333")"""
    // TODO call show command before drop
    sql """DROP PROCEDURE procedure_cursor_select"""
    sql """DROP PROC procedure_insert"""
    // TODO call show command after drop
}
