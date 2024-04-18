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

suite("test_show_create_database", "p0,external,hive,external_docker,external_docker_hive") {

    sql """create database if not exists db_test"""
    result = sql """show create database db_test"""
    assertEquals(result.size(), 1)
    assertEquals(result[0][1], "CREATE DATABASE `db_test`")

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hive2HmsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String catalog_name = "hive_test_other"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        sql """switch ${catalog_name}"""

        result = sql """show create database `default`"""
        assertEquals(result.size(), 1)
        assertTrue(result[0][1].contains("CREATE DATABASE `default` LOCATION 'hdfs:"))

        sql """drop catalog if exists ${catalog_name}"""
    }
}