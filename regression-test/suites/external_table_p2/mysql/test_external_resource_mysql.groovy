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
suite("test_external_resource_mysql", "p2,external,mysql,external_remote,external_remote_mysql") {

    Boolean ignoreP2 = true;
    if (ignoreP2) {
        logger.info("disable p2 test");
        return;
    }

    String enabled = context.config.otherConfigs.get("enableExternalMysqlTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extMysqlHost = context.config.otherConfigs.get("extMysqlHost")
        String extMysqlPort = context.config.otherConfigs.get("extMysqlPort")
        String extMysqlUser = context.config.otherConfigs.get("extMysqlUser")
        String extMysqlPassword = context.config.otherConfigs.get("extMysqlPassword")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
        String mysqlResourceName = "jdbc_resource_mysql_57_y"
        String mysqlDatabaseName01 = "external_mysql_database_ssb"
        String mysqlTableNameLineOrder = "external_mysql_table_lineorder"
        String mysqlTableNameCustomer = "external_mysql_table_customer"
        String mysqlTableNameSupplier = "external_mysql_table_supplier"



        sql """drop database if exists ${mysqlDatabaseName01};"""
        sql """create database ${mysqlDatabaseName01};"""
        sql """use ${mysqlDatabaseName01};"""


        sql """drop resource if exists ${mysqlResourceName};"""
        sql """
            create external resource if not exists ${mysqlResourceName}
            properties (
                "type"="jdbc",
                "user"="${extMysqlUser}",
                "password"="${extMysqlPassword}",
                "jdbc_url"="jdbc:mysql://${extMysqlHost}:${extMysqlPort}/ssb?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&serverTimezone=Asia/Shanghai&useSSL=false",
                "driver_url"="${driver_url}",
                "driver_class"="com.mysql.cj.jdbc.Driver"
            );
            """

        sql """drop table if exists ${mysqlTableNameLineOrder}"""
        sql """
            CREATE EXTERNAL TABLE ${mysqlTableNameLineOrder} (
                  `lo_orderkey` bigint(20) NOT NULL COMMENT "",
                  `lo_linenumber` bigint(20) NOT NULL COMMENT "",
                  `lo_custkey` int(11) NOT NULL COMMENT "",
                  `lo_partkey` int(11) NOT NULL COMMENT "",
                  `lo_suppkey` int(11) NOT NULL COMMENT "",
                  `lo_orderdate` int(11) NOT NULL COMMENT "",
                  `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
                  `lo_shippriority` int(11) NOT NULL COMMENT "",
                  `lo_quantity` bigint(20) NOT NULL COMMENT "",
                  `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
                  `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
                  `lo_discount` bigint(20) NOT NULL COMMENT "",
                  `lo_revenue` bigint(20) NOT NULL COMMENT "",
                  `lo_supplycost` bigint(20) NOT NULL COMMENT "",
                  `lo_tax` bigint(20) NOT NULL COMMENT "",
                  `lo_commitdate` bigint(20) NOT NULL COMMENT "",
                  `lo_shipmode` varchar(11) NOT NULL COMMENT ""
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "${mysqlResourceName}",
            "table" = "lineorder",
            "table_type"="mysql"
            );
            """

        def res = sql """select * from ${mysqlTableNameLineOrder} limit 10;"""
        logger.info("recoding select: " + res.toString())

        sql """drop table if exists ${mysqlTableNameCustomer}"""
        sql """
            CREATE EXTERNAL TABLE ${mysqlTableNameCustomer} (
                    `c_custkey` int(11) DEFAULT NULL,
                    `c_name` varchar(25) NOT NULL,
                    `c_address` varchar(40) NOT NULL,
                    `c_city` varchar(10) NOT NULL,
                    `c_nation` varchar(15) NOT NULL,
                    `c_region` varchar(12) NOT NULL,
                    `c_phone` varchar(15) NOT NULL
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "${mysqlResourceName}",
            "table" = "customer",
            "table_type"="mysql"
            );
            """

        def res1 = sql """select * from ${mysqlTableNameCustomer} where c_custkey >100 limit 10;"""
        logger.info("recoding select: " + res1.toString())

        def res2 = sql """select * from ${mysqlTableNameCustomer} order by c_custkey desc limit 10;"""
        logger.info("recoding select: " + res2.toString())

//        def res3 = sql """select AVG(lo_discount) from ${mysqlTableNameCustomer} limit 10;"""
//        logger.info("recoding select: " + res3.toString())
//
//        def res4 = sql """select MAX(lo_discount) from ${mysqlTableNameCustomer} limit 10;"""
//        logger.info("recoding select: " + res4.toString())

        def res5 = sql """select count(*) from ${mysqlTableNameCustomer};"""
        logger.info("recoding select: " + res5.toString())

        sql """drop table if exists ${mysqlTableNameSupplier}"""
        sql """
            CREATE EXTERNAL TABLE ${mysqlTableNameSupplier} (
                `s_suppkey` int(11) DEFAULT NULL,
                `s_name` varchar(25) NOT NULL,
                `s_address` varchar(25) NOT NULL,
                `s_city` varchar(10) NOT NULL,
                `s_nation` varchar(15) NOT NULL,
                `s_region` varchar(12) NOT NULL,
                `s_phone` varchar(15) NOT NULL
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "${mysqlResourceName}",
            "table" = "supplier",
            "table_type"="mysql"
            );
            """
        def res6 = sql """select count(*) from ${mysqlTableNameSupplier};"""
        logger.info("recoding select: " + res6.toString())

        def res7 = sql """select * from ${mysqlTableNameCustomer} a  join ${mysqlTableNameSupplier} b on a.c_nation =b.s_nation limit 5;"""
        logger.info("recoding select: " + res7.toString())


        sql """drop table if exists ${mysqlTableNameLineOrder}"""
        sql """drop database if exists ${mysqlDatabaseName01};"""
    }
}








