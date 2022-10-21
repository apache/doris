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

suite("test_jdbc_query_pg", "p0") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_14_port = context.config.otherConfigs.get("pg_14_port")
        String jdbcResourcePg14 = "jdbc_resource_pg_14"
        String jdbcPg14Table1 = "jdbc_pg_14_table1"

        sql """drop resource if exists $jdbcResourcePg14;"""
        sql """
            create external resource $jdbcResourcePg14
            properties (
                "type"="jdbc",
                "user"="postgres",
                "password"="123456",
                "jdbc_url"="jdbc:postgresql://127.0.0.1:$pg_14_port/postgres?currentSchema=doris_test",
                "driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/postgresql-42.5.0.jar",
                "driver_class"="org.postgresql.Driver"
            );
            """

        sql """drop table if exists $jdbcPg14Table1"""
        sql """
            CREATE EXTERNAL TABLE `$jdbcPg14Table1` (
                k1 boolean,
                k2 char(100),
                k3 varchar(128),
                k4 date,
                k5 double,
                k6 smallint,
                k7 int,
                k8 bigint,
                k9 datetime,
                k10 decimal(10, 3)
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourcePg14",
            "table" = "test1",
            "table_type"="postgresql"
            );
            """
        order_qt_sql1 """select count(*) from $jdbcPg14Table1"""
        order_qt_sql2 """select * from $jdbcPg14Table1"""
    }
}

