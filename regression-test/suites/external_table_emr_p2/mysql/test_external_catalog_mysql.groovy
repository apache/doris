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
//import com.mysql.cj.jdbc.Driver
suite("test_external_catalog_mysql", "p2") {

    String enabled = context.config.otherConfigs.get("enableExternalMysqlTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extMysqlHost = context.config.otherConfigs.get("extMysqlHost")
        String extMysqlPort = context.config.otherConfigs.get("extMysqlPort")
        String extMysqlUser = context.config.otherConfigs.get("extMysqlUser")
        String extMysqlPassword = context.config.otherConfigs.get("extMysqlPassword")
        String mysqlDatabaseName01 = "external_mysql_catalog_database01"
        String mysqlResource01 = "mysql_catalog_resource_01"
        String mysqlCatalogName = "mysql_jdbc"
        String mysqlTableName01 = "lineorder"
        String mysqlTableName02 = "customer"
        String mysqlTableName03 = "supplier"

        sql """drop database if exists ${mysqlDatabaseName01};"""
        sql """create database ${mysqlDatabaseName01};"""
        sql """use ${mysqlDatabaseName01};"""


        sql """drop resource if exists ${mysqlResource01};"""
        sql """
            CREATE RESOURCE ${mysqlResource01}
            properties (
                "type"="jdbc",
                "user"="${extMysqlUser}",
                "password"="${extMysqlPassword}",
                "jdbc_url"="jdbc:mysql://${extMysqlHost}:${extMysqlPort}/ssb?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&serverTimezone=Asia/Shanghai&useSSL=false",
                "driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
                "driver_class"="com.mysql.cj.jdbc.Driver"
            );
            """

        sql """drop catalog if exists ${mysqlCatalogName};"""

        sql """CREATE CATALOG ${mysqlCatalogName} WITH RESOURCE ${mysqlResource01};"""
//        sql """drop catalog if exists ${mysqlCatalogName};"""


//        sql """
//            CREATE CATALOG ${mysqlCatalogName}
//            PROPERTIES(
//                "type"="jdbc",
//                "jdbc.user"="${extMysqlUser}",
//                "jdbc.password"="${extMysqlPassword}",
//                "jdbc.jdbc_url"="jdbc:mysql://${extMysqlHost}:${extMysqlPort}/ssb?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&serverTimezone=Asia/Shanghai&useSSL=false",
//                "jdbc.driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
//                "jdbc.driver_class"="com.mysql.cj.jdbc.Driver"
//            );
//            """

        sql """switch ${mysqlCatalogName}"""

        sql """use ssb;"""


        def res = sql """select count(*) from ${mysqlTableName02};"""
        logger.info("recoding select: " + res.toString())


        def res1 = sql """select count(*) from ${mysqlTableName03};"""
        logger.info("recoding select: " + res1.toString())

        def res2 = sql """select * from ${mysqlTableName02} limit 10;"""
        logger.info("recoding select: " + res2.toString())

        def res3 = sql """select * from ${mysqlTableName03} limit 10;"""
        logger.info("recoding select: " + res3.toString())

        def res4 = sql """select * from ${mysqlTableName02} a  join ${mysqlTableName03} b on a.c_nation =b.s_nation limit 5;"""
        logger.info("recoding select: " + res4.toString())


//        sql """drop table if exists ${mysqlTableName01}"""
        sql """drop database if exists ${mysqlDatabaseName01};"""
        sql """switch internal;"""
    }
}







