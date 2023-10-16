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

suite("test_prepare_hive_data_in_case", "p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get("hms_port")

            hive_docker """show databases;"""
            hive_docker """drop table if exists default.test_prepare_hive_data_in_case;"""
            hive_docker """create table default.test_prepare_hive_data_in_case (k1 String, k2 String);"""
            hive_docker """insert into default.test_prepare_hive_data_in_case values ('aaa','bbb'),('ccc','ddd'),('eee','fff')"""
            def values = hive_docker """select count(*) from `default`.test_prepare_hive_data_in_case;"""
            log.info(values.toString())

            sql """drop catalog if exists test_prepare_hive_data_in_case;"""
            sql """CREATE CATALOG test_prepare_hive_data_in_case PROPERTIES (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            def values2 = sql """select count(*) from test_prepare_hive_data_in_case.`default`.test_prepare_hive_data_in_case;"""
            log.info(values2.toString())
            assertEquals(values[0][0],values2[0][0])

            qt_hive_docker_01 """select * from default.test_prepare_hive_data_in_case order by k1 desc """

        } finally {
        }
    }
}

