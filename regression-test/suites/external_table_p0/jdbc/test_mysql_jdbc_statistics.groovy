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

suite("test_mysql_jdbc_statistics", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "test_mysql_jdbc_statistics";

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """use ${catalog_name}.doris_test"""
        sql """analyze table ex_tb0 with sync"""
        def result = sql """show column stats ex_tb0 (name)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "name")
        assertTrue(result[0][1] == "5.0")
        assertTrue(result[0][2] == "5.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "15.0")
        assertTrue(result[0][5] == "3.0")
        assertEquals(result[0][6], "'abc'")
        assertEquals(result[0][7], "'abg'")

        result = sql """show column stats ex_tb0 (id)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "id")
        assertTrue(result[0][1] == "5.0")
        assertTrue(result[0][2] == "5.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "20.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "111")
        assertTrue(result[0][7] == "115")

        sql """drop catalog ${catalog_name}"""
    }
}

