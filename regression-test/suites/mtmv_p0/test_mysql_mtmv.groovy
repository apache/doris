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

suite("test_mysql_mtmv", "p0,external,mysql,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    logger.info("enabled: " + enabled)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    logger.info("externalEnvIp: " + externalEnvIp)
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    logger.info("mysql_port: " + mysql_port)
    String s3_endpoint = getS3Endpoint()
    logger.info("s3_endpoint: " + s3_endpoint)
    String bucket = getS3BucketName()
    logger.info("bucket: " + bucket)
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    logger.info("driver_url: " + driver_url)
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "mysql_mtmv_catalog";
        String mvName = "test_mysql_mtmv"
        String dbName = "regression_test_mtmv_p0"
        String mysqlDb = "doris_test"
        String mysqlTable = "ex_tb2"
        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/${mysqlDb}?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        qt_catalog """select * from ${catalog_name}.${mysqlDb}.${mysqlTable} order by id"""
        sql """drop materialized view if exists ${mvName};"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH COMPLETE ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${catalog_name}.${mysqlDb}.${mysqlTable};
            """

        sql """
                REFRESH MATERIALIZED VIEW ${mvName} AUTO
            """
        def jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv "SELECT * FROM ${mvName} order by id"

        sql """drop materialized view if exists ${mvName};"""
        sql """ drop catalog if exists ${catalog_name} """
    }
}

