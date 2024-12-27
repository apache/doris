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

suite("test_lower_case_meta_with_lower_table_conf_auth", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    String user = "test_catalog_lower_case_auth_user"
    String pwd = 'C123_567p'

    sql """drop database if exists internal.external_test_lower_with_conf_auth; """
    sql """drop database if exists internal.external_test_UPPER_with_conf_auth; """
    sql """create database if not exists internal.external_test_lower_with_conf_auth; """
    sql """create database if not exists internal.external_test_UPPER_with_conf_auth; """
    sql """create table if not exists internal.external_test_lower_with_conf_auth.lower_with_conf
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """create table if not exists internal.external_test_lower_with_conf_auth.UPPER_with_conf
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """create table if not exists internal.external_test_UPPER_with_conf_auth.lower_with_conf
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """create table if not exists internal.external_test_UPPER_with_conf_auth.UPPER_with_conf
            (id int, name varchar(20))
            distributed by hash(id) buckets 10
            properties('replication_num' = '1'); 
            """

    sql """insert into internal.external_test_lower_with_conf_auth.lower_with_conf values(1, 'lower')"""
    sql """insert into internal.external_test_lower_with_conf_auth.UPPER_with_conf values(1, 'UPPER')"""

    sql """insert into internal.external_test_UPPER_with_conf_auth.lower_with_conf values(1, 'lower')"""
    sql """insert into internal.external_test_UPPER_with_conf_auth.UPPER_with_conf values(1, 'UPPER')"""

    // 
    sql """drop catalog if exists test_lower_false_with_conf0_auth """

    sql """ CREATE CATALOG `test_lower_false_with_conf0_auth` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf_auth,external_test_UPPER_with_conf_auth",
            "lower_case_meta_names" = "false"
        )"""


    sql """ show tables from test_lower_false_with_conf0_auth.external_test_lower_with_conf_auth;"""
    sql """ show tables from test_lower_false_with_conf0_auth.external_test_UPPER_with_conf_auth;"""

    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    sql """grant select_priv on regression_test to ${user}"""

    sql """grant select_priv on test_lower_false_with_conf0_auth.external_test_lower_with_conf_auth.lower_with_conf to ${user}"""
    sql """grant select_priv on test_lower_false_with_conf0_auth.external_test_lower_with_conf_auth.UPPER_with_conf to ${user}"""
    sql """grant select_priv on test_lower_false_with_conf0_auth.external_test_UPPER_with_conf_auth.lower_with_conf to ${user}"""
    sql """grant select_priv on test_lower_false_with_conf0_auth.external_test_UPPER_with_conf_auth.UPPER_with_conf to ${user}"""

    // db
    connect(user,"${pwd}", context.config.jdbcUrl) {
        def showDbRes = sql """show databases from test_lower_false_with_conf0_auth;"""
        logger.info("showDbRes: " + showDbRes.toString())
        assertTrue(showDbRes.toString().contains("external_test_lower_with_conf_auth"))
        assertTrue(showDbRes.toString().contains("external_test_UPPER_with_conf_auth"))
    }

    // table
    connect(user,"${pwd}", context.config.jdbcUrl) {
        def showTableRes = sql """show tables from test_lower_false_with_conf0_auth.external_test_lower_with_conf_auth;"""
        logger.info("showTableRes: " + showTableRes.toString())
        assertTrue(showTableRes.toString().contains("lower_with_conf"))
        assertTrue(showTableRes.toString().contains("UPPER_with_conf"))
    }

    sql """ drop catalog if exists test_lower_true_with_conf0_auth """
    sql """ CREATE CATALOG `test_lower_true_with_conf0_auth` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf_auth,external_test_UPPER_with_conf_auth",
            "lower_case_meta_names" = "true"
        )"""

    sql """ show tables from test_lower_true_with_conf0_auth.external_test_lower_with_conf_auth;"""
    sql """ show tables from test_lower_true_with_conf0_auth.external_test_upper_with_conf_auth;"""

    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    sql """grant select_priv on regression_test to ${user}"""

    sql """grant select_priv on test_lower_true_with_conf0_auth.external_test_lower_with_conf_auth.lower_with_conf to ${user}"""
    sql """grant select_priv on test_lower_true_with_conf0_auth.external_test_lower_with_conf_auth.upper_with_conf to ${user}"""
    sql """grant select_priv on test_lower_true_with_conf0_auth.external_test_upper_with_conf_auth.lower_with_conf to ${user}"""
    sql """grant select_priv on test_lower_true_with_conf0_auth.external_test_upper_with_conf_auth.upper_with_conf to ${user}"""

    // db
    connect(user,"${pwd}", context.config.jdbcUrl) {
        def showDbRes = sql """show databases from test_lower_true_with_conf0_auth;"""
        logger.info("showDbRes: " + showDbRes.toString())
        assertTrue(showDbRes.toString().contains("external_test_lower_with_conf_auth"))
        assertTrue(showDbRes.toString().contains("external_test_upper_with_conf_auth"))
    }

    // table
    connect(user,"${pwd}", context.config.jdbcUrl) {
        def showTableRes = sql """show tables from test_lower_true_with_conf0_auth.external_test_lower_with_conf_auth;"""
        logger.info("showTableRes: " + showTableRes.toString())
        assertTrue(showTableRes.toString().contains("lower_with_conf"))
        assertTrue(showTableRes.toString().contains("upper_with_conf"))
    }

    sql """ drop catalog if exists test_lower_true_with_conf0_auth """


    try_sql("DROP USER ${user}")

    sql """drop database if exists internal.external_test_lower_with_conf_auth; """
}
