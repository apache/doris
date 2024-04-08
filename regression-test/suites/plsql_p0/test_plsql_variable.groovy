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

suite("test_plsql_variable") {
    def tableName = "plsql_variable"
    sql """
        CREATE OR REPLACE PROCEDURE plsql_variable1()
        BEGIN
            DECLARE a STRING;
            a:="hello world!";
            PRINT a;
            PRINT upper(a);

            DECLARE b = 10;
            PRINT b;
            b = length(a);
            PRINT b;

            DECLARE c = a;
            PRINT c;
            c:=b;
            PRINT c;
            c = "hello kudo!"
            PRINT c;

            DECLARE d STRING;
            PRINT d;
            d = NOW();
            PRINT d;
        END;
        """
    qt_select """call plsql_variable1()"""

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """

    sql """
        CREATE OR REPLACE PROCEDURE plsql_variable_insert(IN id int, IN name STRING)
        BEGIN
            INSERT INTO ${tableName} VALUES(id, name);
        END;
        """
    sql """call plsql_variable_insert(111, "plsql111")"""
    sql """call plsql_variable_insert(222, "plsql222")"""
    sql """call plsql_variable_insert(333, "plsql333")"""
    sql """call plsql_variable_insert(111, "plsql333")"""
    qt_select "select sum(id), count(1) from ${tableName}"

    sql """
        CREATE OR REPLACE PROCEDURE plsql_variable2()
        BEGIN
            DECLARE a int = 2;
            DECLARE b string = "  plsql111   ";
            print a;
            print b;
            print trim(b);

            DECLARE c string;
            select name into c from ${tableName} where 2=a and name=trim(b);
            print c;

            DECLARE d int;
            select count(1) into d from ${tableName} where 2=a;
            print d;
        END;
        """
    qt_select """call plsql_variable2()"""

    // TODO, currently, variable take priority over column, Oracle column priority.
    sql """
        CREATE OR REPLACE PROCEDURE plsql_variable3()
        BEGIN
            DECLARE a int = 999;
            print a;

            DECLARE b int;
            select id into b from ${tableName} where 999=a limit 1;
            print b;

            DECLARE id int = 999;
            print id;

            DECLARE c string;
            select id into c from ${tableName} where 999=id limit 1;
            print c;

            DECLARE d string;
            select count(1) into d from ${tableName} where 999=id;
            print d;
        END;
        """
    qt_select """call plsql_variable3()"""

    sql """
        CREATE OR REPLACE PROCEDURE plsql_variable4()
        BEGIN
            select 1;     
            select now();
            select to_date("2024-04-07 00:00:00");
            select 9999 * 999 + 99 / 9;
        END;
        """
    // qt_select """call plsql_variable4()""" // Groovy jdbc not support procedure return select results.

    sql "DROP TABLE IF EXISTS plsql_variable2"
    sql """
        create table plsql_variable2 (k1 int, k2 varchar(20), k3 double) DUPLICATE key(`k1`) distributed by hash (`k1`) buckets 1
        properties ("replication_num"="1");
        """
    sql """
        CREATE OR REPLACE PROCEDURE plsql_variable5()
        BEGIN
            INSERT INTO plsql_variable2 select 1, to_date("2024-04-07 00:00:00"), 9999 * 999 + 99 / 9;
        END;
        """
    qt_select """call plsql_variable5()"""
    qt_select "select * from plsql_variable2"

    sql """DROP PROCEDURE plsql_variable1"""
    sql """DROP PROCEDURE plsql_variable2"""
    sql """DROP PROCEDURE plsql_variable3"""
    sql """DROP PROCEDURE plsql_variable4"""
    sql """DROP PROCEDURE plsql_variable5"""
    sql """DROP PROC plsql_variable_insert"""
}
