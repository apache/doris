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
//import org.postgresql.Driver
suite("test_external_pg_nereids", "p2,external,pg,external_remote,external_remote_pg") {

    Boolean ignoreP2 = true;
    if (ignoreP2) {
        logger.info("disable p2 test");
        return;
    }

    String enabled = context.config.otherConfigs.get("enableExternalPgTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extPgHost = context.config.otherConfigs.get("extPgHost")
        String extPgPort = context.config.otherConfigs.get("extPgPort")
        String extPgUser = context.config.otherConfigs.get("extPgUser")
        String extPgPassword = context.config.otherConfigs.get("extPgPassword")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"
        String jdbcResourcePg14 = "jdbc_resource_pg_14_n"
        String jdbcPg14Database1 = "jdbc_pg_14_database1_n"
        String pgTableNameLineOrder = "jdbc_pg_14_table1_n"
        String pgTableNameCustomer = "jdbc_pg_14_customer_n"
        String pgTableNameSupplier = "jdbc_pg_14_supplier_n"

        sql """set enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""
        sql """drop database if exists ${jdbcPg14Database1};"""
        sql """drop resource if exists ${jdbcResourcePg14};"""

        sql """drop database if exists ${jdbcPg14Database1};"""
        sql """create database ${jdbcPg14Database1};"""
        sql """use ${jdbcPg14Database1};"""
        sql """
            create external resource ${jdbcResourcePg14}
            properties (
                "type"="jdbc",
                "user"="${extPgUser}",
                "password"="${extPgPassword}",
                "jdbc_url"="jdbc:postgresql://${extPgHost}:${extPgPort}/ssb?currentSchema=ssb&useCursorFetch=true",
                "driver_url"="${driver_url}",
                "driver_class"="org.postgresql.Driver"
            );
            """

        sql """drop table if exists ${pgTableNameLineOrder}"""
        sql """
            CREATE EXTERNAL TABLE ${pgTableNameLineOrder} (
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
            "resource" = "${jdbcResourcePg14}",
            "table" = "lineorder",
            "table_type"="postgresql"
            );
            """

        sql """drop table if exists ${pgTableNameCustomer}"""
        sql """
            CREATE EXTERNAL TABLE ${pgTableNameCustomer} (
                    `c_custkey` int(11) DEFAULT NULL,
                    `c_name` varchar(25) NOT NULL,
                    `c_address` varchar(40) NOT NULL,
                    `c_city` varchar(10) NOT NULL,
                    `c_nation` varchar(15) NOT NULL,
                    `c_region` varchar(12) NOT NULL,
                    `c_phone` varchar(15) NOT NULL
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "${jdbcResourcePg14}",
            "table" = "customer",
            "table_type"="postgresql"
            );
            """

        sql """drop table if exists ${pgTableNameSupplier}"""
        sql """
            CREATE EXTERNAL TABLE ${pgTableNameSupplier} (
                `s_suppkey` int(11) DEFAULT NULL,
                `s_name` varchar(25) NOT NULL,
                `s_address` varchar(25) NOT NULL,
                `s_city` varchar(10) NOT NULL,
                `s_nation` varchar(15) NOT NULL,
                `s_region` varchar(12) NOT NULL,
                `s_phone` varchar(15) NOT NULL
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "${jdbcResourcePg14}",
            "table" = "supplier",
            "table_type"="postgresql"
            );
            """

        def res = sql """select count(*) from ${pgTableNameCustomer};"""
        logger.info("recoding select: " + res.toString())

        def res1 = sql """select * from ${pgTableNameSupplier} limit 10"""
        logger.info("recoding select: " + res1.toString())

        def res2 = sql """select * from ${pgTableNameSupplier} order by s_suppkey desc limit 10;"""
        logger.info("recoding select: " + res2.toString())

        def res3 = sql """select * from ${pgTableNameSupplier} where s_suppkey>100 limit 10;"""
        logger.info("recoding select: " + res3.toString())

        def res4 = sql """select * from ${pgTableNameCustomer} a  join ${pgTableNameSupplier} b on a.c_nation =b.s_nation limit 5;"""
        logger.info("recoding select: " + res4.toString())

    }
}
