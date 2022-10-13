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

suite("test_jdbc_query_mysql", "p0") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_57_port = context.config.otherConfigs.get("mysql_57_port")
        String jdbcResourceMysql57 = "jdbc_resource_mysql_57"
        String jdbcMysql57Table1 = "jdbc_mysql_57_table1"

        sql """drop resource if exists $jdbcResourceMysql57;"""
        sql """
            create external resource $jdbcResourceMysql57
            properties (
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url"="jdbc:mysql://127.0.0.1:$mysql_57_port/doris_test",
                "driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
                "driver_class"="com.mysql.cj.jdbc.Driver"
            );
            """

        sql """drop table if exists $jdbcMysql57Table1"""
        sql """
            CREATE EXTERNAL TABLE `$jdbcMysql57Table1` (
                k1 boolean,
                k2 char(100),
                k3 varchar(128),
                k4 date,
                k5 float,
                k6 tinyint,
                k7 smallint,
                k8 int,
                k9 bigint,
                k10 double,
                k11 datetime,
                k12 decimal(10, 3)
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "$jdbcResourceMysql57",
            "table" = "test1",
            "table_type"="mysql"
            );
            """
        order_qt_sql1 """select count(*) from $jdbcMysql57Table1"""
        order_qt_sql2 """select * from $jdbcMysql57Table1"""
    }
}

