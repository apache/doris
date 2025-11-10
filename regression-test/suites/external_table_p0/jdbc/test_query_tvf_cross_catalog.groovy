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

suite("test_query_tvf_cross_catalog", "p0,external,mysql,external_docker,external_docker_mysql") {
    String jdbcUrl = context.config.jdbcUrl + "&jdbcCompliantTruncation=false&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop catalog if exists test_query_tvf_cross_catalog_mysql """
        sql """create catalog if not exists test_query_tvf_cross_catalog_mysql properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """ DROP CATALOG IF EXISTS test_query_tvf_cross_catalog_doris """
        sql """ CREATE CATALOG test_query_tvf_cross_catalog_doris PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
            )
        """

        sql """create database if not exists internal.test_query_tvf_cross_catalog_db;"""
        sql """use internal.test_query_tvf_cross_catalog_db;"""
        sql """drop table if exists order_qt_cross_catalog_tvf_join;"""
        sql """
            create table order_qt_cross_catalog_tvf_join (
                k1 int
            ) distributed by hash(k1) buckets 1 properties("replication_num" = "1");
        """
        sql """insert into order_qt_cross_catalog_tvf_join values (1);"""

        order_qt_cross_catalog_tvf_join """
            select *  from 
            query("catalog" = "test_query_tvf_cross_catalog_mysql", "query" = "select * from doris_test.ex_tb0 limit 10") a 
            left join query("catalog" = "test_query_tvf_cross_catalog_doris", "query" = "select * from test_query_tvf_cross_catalog_db.order_qt_cross_catalog_tvf_join limit 10") b 
            on a.id = b.k1;
            """
        
        sql """drop table if exists order_qt_cross_catalog_tvf_join;"""
        sql """drop database if exists internal.test_query_tvf_cross_catalog_db;"""
        sql """drop catalog if exists test_query_tvf_cross_catalog_mysql """
        sql """drop catalog if exists test_query_tvf_cross_catalog_doris """
    }
}