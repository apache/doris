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

suite("test_outline_param") {
    sql """set enable_sql_plan_outlines=true;"""
    String dbName = "test_outline_param"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE ${dbName}"
    String tableName = "t1"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """create table ${tableName} (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""

    sql """drop outline if exists ${dbName}_outline1"""
    sql """create outline ${dbName}_outline1 on select * from ${tableName} where c1 = 1"""
    qt_sql """select outline_name, visible_signature from information_schema.optimizer_sql_plan_outline where outline_name = "${dbName}_outline1";"""
    sql """select * from ${tableName} where c1 = 1"""
    sql """drop outline if exists ${dbName}_outline2"""
    sql """create outline ${dbName}_outline2 on select * from ${tableName} order by 1"""
    qt_sql """select outline_name, visible_signature from information_schema.optimizer_sql_plan_outline where outline_name = "${dbName}_outline2";"""
    sql """drop outline if exists ${dbName}_outline3"""
    sql """create outline ${dbName}_outline3 on select c1, sum(c1) from ${tableName} group by 1"""
    qt_sql """select outline_name, visible_signature from information_schema.optimizer_sql_plan_outline where outline_name = "${dbName}_outline3";"""

    sql """drop outline if exists ${dbName}_outline1"""
    sql """drop outline if exists ${dbName}_outline2"""
    sql """drop outline if exists ${dbName}_outline3"""

    sql """drop database ${dbName}"""
}
