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

suite("test_jdbc_mysql_catalog_table_comment") {
    String ex_table_name = "test_table_comment";
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // mysql catalog
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    for (String driver_class : ["com.mysql.cj.jdbc.Driver","com.mysql.jdbc.Driver" ]) {
        if (driver_class.equals("com.mysql.jdbc.Driver")) {
            driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-5.1.49.jar"
        } else  {
            driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
        }
        String user = "test_jdbc_user";
        String pwd = '123456';
        String catalog_name = "mysql_jdbc_catalog";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "${driver_class}"
        );"""

        sql """switch ${catalog_name}"""
        sql """ use ${ex_db_name}"""

        explain {
            sql "SHOW CREATE TABLE ${ex_table_name}"
            contains "test table comment"
        }
    }

    // doris catalog
    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_doris_catalog"
    String jdbcPassword = "C123_567p"
    driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${jdbcUser}""";
    }

    sql """grant all on *.*.* to ${jdbcUser}"""

    sql """drop database if exists internal.EXTERNAL_DORIS; """
    sql """create database if not exists internal.EXTERNAL_DORIS;"""
    sql """create table if not exists internal.EXTERNAL_DORIS.TABLE_TEST
         (id int, name varchar(20))
         COMMENT 'test table comment'
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """drop catalog if exists test_doris_catalog """

    sql """ CREATE CATALOG `test_doris_catalog` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "EXTERNAL_DORIS"
        )"""

    sql """switch test_doris_catalog"""
    sql """ use external_doris"""

    explain {
        sql "SHOW CREATE TABLE table_test"
        contains "test table comment"
    }

    sql """drop catalog if exists test_doris_catalog """
    sql """drop database if exists internal.EXTERNAL_DORIS"""

    try_sql """drop user ${jdbcUser}"""


}


