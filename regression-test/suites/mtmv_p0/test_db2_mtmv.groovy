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

suite("test_db2_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    logger.info("enabled: " + enabled)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    logger.info("externalEnvIp: " + externalEnvIp)
    String s3_endpoint = getS3Endpoint()
    logger.info("s3_endpoint: " + s3_endpoint)
    String bucket = getS3BucketName()
    logger.info("bucket: " + bucket)
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/jcc-11.5.8.0.jar"
    logger.info("driver_url: " + driver_url)
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "db2_mtmv_catalog";
        String mvName = "db2_mtmv";
        String dbName = "regression_test_mtmv_p0";
        String db2_database = "DORIS_MTMV";
        String db2_port = context.config.otherConfigs.get("db2_11_port");
        String db2_table = "USER";

        try {
            logger.info("CREATE SCHEMA")
            db2_docker "CREATE SCHEMA ${db2_database};"
            logger.info("CREATE TABLE")
            db2_docker """CREATE TABLE ${db2_database}.${db2_table} (
                k1 INT GENERATED ALWAYS AS IDENTITY,
                k2 INT
            );"""
            logger.info("INSERT INTO")
            db2_docker """INSERT INTO ${db2_database}.${db2_table} (
                k2
            ) VALUES (
                1
            );"""

            sql """drop catalog if exists ${catalog_name} """

            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "user"="db2inst1",
                "password"="123456",
                "jdbc_url" = "jdbc:db2://${externalEnvIp}:${db2_port}/doris",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.ibm.db2.jcc.DB2Driver"
            );"""

            qt_catalog """select * from ${catalog_name}.${db2_database}.${db2_table} order by k1"""

            sql """drop materialized view if exists ${mvName};"""

            sql """
                CREATE MATERIALIZED VIEW ${mvName}
                    BUILD DEFERRED REFRESH AUTO ON MANUAL
                    DISTRIBUTED BY RANDOM BUCKETS 2
                    PROPERTIES ('replication_num' = '1')
                    AS
                    SELECT * FROM ${catalog_name}.${db2_database}.${db2_table};
                """

            sql """
                    REFRESH MATERIALIZED VIEW ${mvName} complete
                """
            def jobName = getJobName(dbName, mvName);
            waitingMTMVTaskFinished(jobName)
            order_qt_mtmv_1 "SELECT * FROM ${mvName} order by k1"

            logger.info("INSERT INTO")
                    db2_docker """INSERT INTO ${db2_database}.${db2_table} (
                        k2
                    ) VALUES (
                        2
                    );"""
            sql """
                    REFRESH MATERIALIZED VIEW ${mvName} complete
                """
            waitingMTMVTaskFinished(jobName)
            order_qt_mtmv_2 "SELECT * FROM ${mvName} order by k1"

            sql """ drop catalog if exists ${catalog_name} """
            db2_docker "DROP TABLE IF EXISTS ${db2_database}.${db2_table};"
            db2_docker "DROP SCHEMA ${db2_database} restrict;"

        } catch (Exception e) {
            e.printStackTrace()
        }
    }
}

