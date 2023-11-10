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

suite("test_hive_statistic_clean", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_statistic_clean"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
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
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "46.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "50")

        result = sql """show column stats `statistics` (lo_orderkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_orderkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "98")

        result = sql """show column stats `statistics` (lo_linenumber)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_linenumber")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "7.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "7")

        sql """drop expired stats"""
        result = sql """show column stats `statistics` (lo_quantity)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_quantity")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "46.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "50")

        result = sql """show column stats `statistics` (lo_orderkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_orderkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "98")

        result = sql """show column stats `statistics` (lo_linenumber)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_linenumber")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "7.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "7")

        def ctlId
        result = sql """show catalogs"""

        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == catalog_name) {
                ctlId = result[i][0]
            }
        }

        sql """drop catalog ${catalog_name}"""
        sql """drop expired stats"""
        result = sql """select * from internal.__internal_schema.column_statistics where catalog_id=${ctlId}"""
        assertEquals(result.size(), 0)

    }
}

