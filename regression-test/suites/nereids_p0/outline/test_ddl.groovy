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

suite("test_ddl") {
    String dbName = "test_outline_ddl"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE ${dbName}"
    String tableName = "t1"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """create table ${tableName} (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""

    sql """drop outline if exists outline1"""
    sql """drop outline if exists outline2"""
    sql """create outline outline1 on select * from ${tableName}"""
    sql """create outline outline2 on select * from ${tableName}"""
    qt_sql """select outline_name from information_schema.optimizer_sql_plan_outline order by outline_name;"""
    qt_sql """select outline_name from information_schema.optimizer_sql_plan_outline where outline_name = "outline1" order by outline_name;"""

    // should visible outline1 already exists
    test {
        sql """create outline outline1 on select * from ${tableName}"""
        exception "outline1 already exists"
    }
    // or replace should work
    sql """create or replace outline outline1 on select * from ${tableName}"""
    // test drop
    sql """drop outline if exists outline1"""
    qt_sql """select outline_name from information_schema.optimizer_sql_plan_outline order by outline_name;"""
    sql """create outline outline1 on select * from ${tableName}"""
    // test drop without if exists
    sql """drop outline outline1"""
    test {
        sql """drop outline outline1"""
        exception "outline1 not exists"
    }

    sql """drop database ${dbName}"""
}
