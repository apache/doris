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

suite("test_cdc_stream_tvf", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_cdc_stream_tvf"
    def mysqlDb = "test_cdc_db"

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
        def offset = ""

        // create test
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `name` varchar(200) NOT NULL,
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('A1', 1);"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('B1', 2);"""

            def result = sql_return_maparray "show master status"
            def file = result[0]["File"]
            def position = result[0]["Position"]
            offset = """{"file":"${file}","pos":"${position}"}"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('C1', 3);"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('D1', 4);"""
        }

        log.info("offset: " + offset)
        qt_select_tvf """select * from cdc_stream(
                "type" = "mysql",
                "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "user" = "root",
                "password" = "123456",
                "database" = "${mysqlDb}",
                "table" = "${table1}",
                "offset" = '${offset}'
            )
        """

        test {
            sql """
            select * from cdc_stream(
                "type" = "mysql",
                "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "user" = "root",
                "password" = "123456",
                "database" = "${mysqlDb}",
                "table" = "${table1}",
                "offset" = 'initial')
            """
            exception "Unsupported offset: initial"
        }
    }
}
