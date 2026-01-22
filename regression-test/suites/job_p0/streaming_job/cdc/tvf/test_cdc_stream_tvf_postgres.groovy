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

suite("test_cdc_stream_tvf_postgres", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_normal1"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // create test
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // sql """CREATE SCHEMA IF NOT EXISTS ${pgSchema}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "name" varchar(200),
                  "age" int2,
                  PRIMARY KEY ("name")
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age) VALUES ('A1', 1);"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age) VALUES ('B1', 2);"""
        }

        test {
            sql """
            select * from cdc_stream(
                "type" = "postgres",
                 "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                 "driver_url" = "${driver_url}",
                 "driver_class" = "org.postgresql.Driver",
                 "user" = "${pgUser}",
                 "password" = "${pgPassword}",
                 "database" = "${pgDB}",
                 "schema" = "${pgSchema}",
                "table" = "${table1}",
                "offset" = 'initial')
            """
            exception "Unsupported offset: initial"
        }

        // Here, because PG consumption requires creating a slot first,
        // we only verify whether the execution can be successful.
        def result = sql """
            select * from cdc_stream(
                "type" = "postgres",
                "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                "driver_url" = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user" = "${pgUser}",
                "password" = "${pgPassword}",
                "database" = "${pgDB}",
                "schema" = "${pgSchema}",
                "table" = "${table1}",
                "offset" = 'latest')
            """
        log.info("result:", result)
    }
}
