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

suite("test_show_colums", "p0,external,hive,external_docker,external_docker_hive") {

    sql """create database if not exists db1"""
    sql """drop table if exists db1.test_full_column"""
    sql '''create table db1.test_full_column (
                id int not null,
                name varchar(20) not null
        )
        distributed by hash(id) buckets 4
        properties (
                "replication_num"="1"
        );
        '''
    result = sql """show full columns from db1.test_full_column"""
    assertEquals(result.size(), 2)
    assertEquals(result[0][0], "id")

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hms_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String catalog_name = "hive_test_other"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        sql """switch ${catalog_name}"""

        result = sql """show full columns from internal.db1.test_full_column"""
        assertEquals(result.size(), 2)
        assertEquals(result[0][0], "id")

        sql """switch internal"""

        result = sql """show full columns from internal.db1.test_full_column"""
        assertEquals(result.size(), 2)
        assertEquals(result[1][0], "name")

        sql """drop catalog if exists ${catalog_name}"""
    }
}
