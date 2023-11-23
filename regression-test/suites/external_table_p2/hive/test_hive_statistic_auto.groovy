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

suite("test_hive_statistic_auto", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_statistic_auto"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        // Test analyze table without init.
        sql """analyze database ${catalog_name}.statistics PROPERTIES("use.auto.analyzer"="true")"""
        sql """use ${catalog_name}.statistics"""

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000)
            def result = sql """show column stats `statistics` (lo_quantity)"""
            if (result.size <= 0) {
                continue;
            }
            assertTrue(result.size() == 1)
            assertTrue(result[0][0] == "lo_quantity")
            assertTrue(result[0][1] == "100.0")
            assertTrue(result[0][2] == "46.0")
            assertTrue(result[0][3] == "0.0")
            assertTrue(result[0][4] == "404.0")
            assertTrue(result[0][5] == "4.0")
            assertTrue(result[0][6] == "1")
            assertTrue(result[0][7] == "50")

            result = sql """show column stats `statistics` (lo_orderkey)"""
            if (result.size <= 0) {
                continue;
            }
            assertTrue(result.size() == 1)
            assertTrue(result[0][0] == "lo_orderkey")
            assertTrue(result[0][1] == "100.0")
            assertTrue(result[0][2] == "26.0")
            assertTrue(result[0][3] == "0.0")
            assertTrue(result[0][4] == "404.0")
            assertTrue(result[0][5] == "4.0")
            assertTrue(result[0][6] == "1")
            assertTrue(result[0][7] == "98")

            result = sql """show column stats `statistics` (lo_linenumber)"""
            if (result.size <= 0) {
                continue;
            }
            assertTrue(result.size() == 1)
            assertTrue(result[0][0] == "lo_linenumber")
            assertTrue(result[0][1] == "100.0")
            assertTrue(result[0][2] == "7.0")
            assertTrue(result[0][3] == "0.0")
            assertTrue(result[0][4] == "404.0")
            assertTrue(result[0][5] == "4.0")
            assertTrue(result[0][6] == "1")
            assertTrue(result[0][7] == "7")
        }

        sql """drop catalog ${catalog_name}"""

    }
}

