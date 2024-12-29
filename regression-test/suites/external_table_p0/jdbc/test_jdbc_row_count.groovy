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

suite("test_jdbc_row_count", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    logger.info("enabled " + enabled)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // Test mysql
        String catalog_name = "test_mysql_jdbc_row_count";
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""
        sql """use ${catalog_name}.doris_test"""
        sql """select * from ex_tb0"""
        def result = sql """show table stats ex_tb0"""
        Thread.sleep(1000)
        for (int i = 0; i < 60; i++) {
            result = sql """show table stats ex_tb0""";
            if (result[0][2] != "-1") {
                break;
            }
            logger.info("Table row count not ready yet. Wait 1 second.")
            Thread.sleep(1000)
        }
        assertEquals("5", result[0][2])
        sql """drop catalog ${catalog_name}"""

        // Test pg
        catalog_name = "test_pg_jdbc_row_count";
        driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        sql """drop catalog if exists ${catalog_name} """
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver"
        );"""
        sql """use ${catalog_name}.doris_test"""
        sql """select * from test1"""
        result = sql """show table stats test1"""
        Thread.sleep(1000)
        for (int i = 0; i < 60; i++) {
            result = sql """show table stats test1""";
            if (result[0][2] != "-1") {
                break;
            }
            logger.info("Table row count not ready yet. Wait 1 second.")
            Thread.sleep(1000)
        }
        assertEquals("1026", result[0][2])
        sql """drop catalog ${catalog_name}"""

        // Test sqlserver
        catalog_name = "test_sqlserver_jdbc_row_count";
        driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mssql-jdbc-11.2.3.jre8.jar"
        String sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port");
        sql """drop catalog if exists ${catalog_name} """
        sql """ create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        );"""
        sql """use ${catalog_name}.dbo"""
        sql """select * from student"""
        result = sql """show table stats student"""
        Thread.sleep(1000)
        for (int i = 0; i < 60; i++) {
            result = sql """show table stats student""";
            if (result[0][2] != "-1") {
                break;
            }
            logger.info("Table row count not ready yet. Wait 1 second.")
            Thread.sleep(1000)
        }
        assertEquals("3", result[0][2])
        sql """drop catalog ${catalog_name}"""

        // Test oracle
        catalog_name = "test_oracle_jdbc_row_count";
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String SID = "XE";
        driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc8.jar"
        sql """drop catalog if exists ${catalog_name} """
        sql """
            create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
            );
        """
        sql """use ${catalog_name}.DORIS_TEST"""
        result = sql """show table stats STUDENT"""
        Thread.sleep(1000)
        for (int i = 0; i < 30; i++) {
            result = sql """show table stats STUDENT""";
            if (result[0][2] != "-1") {
                break;
            }
            logger.info("Table row count not ready yet. Wait 1 second.")
            Thread.sleep(1000)
        }
        assertTrue("4".equals(result[0][2]) || "-1".equals(result[0][2]))
        sql """drop catalog ${catalog_name}"""
    }
}

