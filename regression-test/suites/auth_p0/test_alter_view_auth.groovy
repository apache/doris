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

import org.junit.Assert;

suite("test_alter_view_auth","p0,auth") {
    String user = 'test_alter_view_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_alter_view_auth_db'
    String tableName = 'test_alter_view_auth_table'
    String viewName = 'test_alter_view_auth_view'
    try_sql("DROP USER ${user}")
    try_sql """drop table if exists ${dbName}.${tableName}"""
    try_sql """drop view if exists ${dbName}.${viewName}"""
    sql """drop database if exists ${dbName}"""

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """create database ${dbName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """grant select_priv on regression_test to ${user}"""
    sql """create view ${dbName}.${viewName} as select * from ${dbName}.${tableName};"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "alter view ${dbName}.${viewName} as select * from ${dbName}.${tableName};"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Alter_priv"))
        }
    }
    sql """grant Alter_priv on ${dbName}.${viewName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "alter view ${dbName}.${viewName} as select * from ${dbName}.${tableName};"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
        }
    }
    try_sql """drop table if exists ${dbName}.${tableName}"""
    try_sql """drop view if exists ${dbName}.${viewName}"""
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
