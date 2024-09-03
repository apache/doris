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

suite("test_drop_expired_table_stats", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = hivePrefix + "_test_drop_expired_table_stats"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""


        sql """use stats_test"""
        sql """analyze table employee_gz with sync"""
        def result = sql """show table stats employee_gz"""
        assertEquals(1, result.size())

        def ctlId
        def dbId
        def tblId
        result = sql """show catalogs"""

        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == catalog_name) {
                ctlId = result[i][0]
            }
        }
        logger.info("catalog id is " + ctlId)
        result = sql """show proc '/catalogs/$ctlId'"""
        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == 'stats_test') {
                dbId = result[i][0]
            }
        }
        logger.info("db id is " + dbId)
        result = sql """show proc '/catalogs/$ctlId/$dbId'"""
        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == 'employee_gz') {
                tblId = result[i][0]
            }
        }
        logger.info("table id is " + tblId)
        result = sql """show table stats $tblId"""
        logger.info("Table stats " + result)
        assertEquals(1, result.size())

        sql """drop catalog ${catalog_name}"""
        result = sql """show table stats $tblId"""
        logger.info("Table stats " + result)
        assertEquals(1, result.size())

        try {
            sql """drop expired stats"""
        } catch (Exception e) {
            logger.info("Drop expired stats exception. " + e.getMessage())
        }
        result = sql """show table stats $tblId"""
        logger.info("Table stats " + result)
        assertEquals(0, result.size())
    }
}

