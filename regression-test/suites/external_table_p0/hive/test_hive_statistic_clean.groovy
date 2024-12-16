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

suite("test_hive_statistic_clean", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_statistic_clean"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        // sql """analyze database ${catalog_name}.statistics with sync"""
        sql """analyze table ${catalog_name}.statistics.statistics with sync"""
        sql """use ${catalog_name}.statistics"""

        def result = sql """show column stats `statistics` (lo_quantity)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_quantity")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "46.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "50")

        result = sql """show column stats `statistics` (lo_orderkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_orderkey")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "26.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "98")

        result = sql """show column stats `statistics` (lo_linenumber)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_linenumber")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "7.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "7")

        /*
        sql """drop expired stats"""
        result = sql """show column stats `statistics` (lo_quantity)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_quantity")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "46.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "50")

        result = sql """show column stats `statistics` (lo_orderkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_orderkey")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "26.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "98")

        result = sql """show column stats `statistics` (lo_linenumber)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_linenumber")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "7.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "7")
        */

        def ctlId
        result = sql """show catalogs"""

        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == catalog_name) {
                ctlId = result[i][0]
            }
        }

        // sql """drop catalog ${catalog_name}"""
        // sql """drop expired stats"""
        sql """drop stats `statistics`"""
        result = sql """select * from internal.__internal_schema.column_statistics where catalog_id=${ctlId}"""
        assertEquals(result.size(), 0)

    }
}

