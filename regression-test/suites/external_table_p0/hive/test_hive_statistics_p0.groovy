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

suite("test_hive_statistics_p0", "all_types,p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hms_port = context.config.otherConfigs.get("hms_port")
            String catalog_name = "test_hive_statistics_p0"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`stats_test`"""
            sql """analyze database stats_test with sync"""
            def result = sql """show column stats stats_test1(id);"""
            assertEquals(1, result.size())
            assertEquals("id", result[0][0])
            assertEquals("3.0", result[0][1])
            assertEquals("3.0", result[0][2])
            assertEquals("0.0", result[0][3])
            assertEquals("12.0", result[0][4])
            assertEquals("4.0", result[0][5])
            assertEquals("1", result[0][6])
            assertEquals("3", result[0][7])

            result = sql """show column stats stats_test1(value);"""
            assertEquals(1, result.size())
            assertEquals("value", result[0][0])
            assertEquals("3.0", result[0][1])
            assertEquals("3.0", result[0][2])
            assertEquals("0.0", result[0][3])
            assertEquals("15.0", result[0][4])
            assertEquals("5.0", result[0][5])
            assertEquals("\'name1\'" , result[0][6])
            assertEquals("\'name3\'" , result[0][7])

            result = sql """show column stats stats_test2(id);"""
            assertEquals(1, result.size())
            assertEquals("id", result[0][0])
            assertEquals("3.0", result[0][1])
            assertEquals("3.0", result[0][2])
            assertEquals("0.0", result[0][3])
            assertEquals("12.0", result[0][4])
            assertEquals("4.0", result[0][5])
            assertEquals("1", result[0][6])
            assertEquals("3", result[0][7])

            result = sql """show column stats stats_test2(value);"""
            assertEquals(1, result.size())
            assertEquals("value", result[0][0])
            assertEquals("3.0", result[0][1])
            assertEquals("3.0", result[0][2])
            assertEquals("0.0", result[0][3])
            assertEquals("15.0", result[0][4])
            assertEquals("5.0", result[0][5])
            assertEquals("\'name1\'", result[0][6])
            assertEquals("\'name3\'", result[0][7])


            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

