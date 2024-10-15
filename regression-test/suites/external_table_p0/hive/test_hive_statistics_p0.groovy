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
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "test_${hivePrefix}_statistics_p0"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`stats_test`"""
            sql """analyze database stats_test with sync"""

            // Test hive scan node cardinality. Estimated row count.
            for (int i = 0; i < 60; i++) {
                def result = sql """show table stats `${catalog_name}`.`statistics`.`statistics`"""
                logger.info("Table stats " + result)
                if (!"66".equalsIgnoreCase(result[0][2])) {
                    Thread.sleep(1000)
                } else {
                    explain {
                        sql "select count(2) from `${catalog_name}`.`statistics`.`statistics`;"
                        contains "cardinality=66"
                    }
                    break;
                }
            }

            def result = sql """show catalog ${catalog_name}"""
            for (int i = 0; i < result.size(); i++) {
                assertNotEquals("enable.auto.analyze", result[i][0]);
            }
            sql """alter catalog ${catalog_name} set properties ("enable.auto.analyze" = "true");"""
            result = sql """show catalog ${catalog_name}"""
            def flag = false;
            for (int i = 0; i < result.size(); i++) {
                if ("enable.auto.analyze".equalsIgnoreCase((String)result[i][0])
                        && "true".equalsIgnoreCase((String)result[i][1])) {
                    flag = true;
                    logger.info("enable.auto.analyze has been set to true")
                }
            }
            assertTrue(flag);
            sql """alter catalog ${catalog_name} set properties ("enable.auto.analyze" = "false");"""
            result = sql """show catalog ${catalog_name}"""
            for (int i = 0; i < result.size(); i++) {
                if ("enable.auto.analyze".equalsIgnoreCase((String)result[i][0])) {
                    assertNotEquals("true", ((String)result[i][1]).toLowerCase());
                    logger.info("enable.auto.analyze has been set back to false")
                }
            }

            result = sql """show column stats stats_test1(id);"""
            assertEquals(1, result.size())
            assertEquals("id", result[0][0])
            assertEquals("3.0", result[0][2])
            assertEquals("3.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("12.0", result[0][5])
            assertEquals("4.0", result[0][6])
            assertEquals("1", result[0][7])
            assertEquals("3", result[0][8])
            assertEquals("FULL" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])

            result = sql """show column stats stats_test1(value);"""
            assertEquals(1, result.size())
            assertEquals("value", result[0][0])
            assertEquals("3.0", result[0][2])
            assertEquals("3.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("15.0", result[0][5])
            assertEquals("5.0", result[0][6])
            assertEquals("\'name1\'" , result[0][7])
            assertEquals("\'name3\'" , result[0][8])
            assertEquals("FULL" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])

            result = sql """show column stats stats_test2(id);"""
            assertEquals(1, result.size())
            assertEquals("id", result[0][0])
            assertEquals("2.0", result[0][2])
            assertEquals("2.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("8.0", result[0][5])
            assertEquals("4.0", result[0][6])
            assertEquals("1", result[0][7])
            assertEquals("2", result[0][8])
            assertEquals("FULL" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])

            result = sql """show column stats stats_test2(value);"""
            assertEquals(1, result.size())
            assertEquals("value", result[0][0])
            assertEquals("2.0", result[0][2])
            assertEquals("2.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("2.0", result[0][5])
            assertEquals("1.0", result[0][6])
            assertEquals("\'*\'", result[0][7])
            assertEquals("\';\'", result[0][8])
            assertEquals("FULL" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])

            sql """drop catalog if exists ${catalog_name}"""

            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`stats_test`"""
            sql """analyze table stats_test1(value) with sync"""
            result = sql """show column stats stats_test1(value);"""
            assertEquals(1, result.size())
            assertEquals("value", result[0][0])
            assertEquals("3.0", result[0][2])
            assertEquals("3.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("15.0", result[0][5])
            assertEquals("5.0", result[0][6])
            assertEquals("\'name1\'" , result[0][7])
            assertEquals("\'name3\'" , result[0][8])
            assertEquals("FULL" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])

            result = sql """show column stats stats_test1(id);"""
            assertEquals(0, result.size())

            sql """analyze table stats_test1(id) with sync with sample rows 100"""
            result = sql """show column stats stats_test1(id);"""
            assertEquals(1, result.size())
            assertEquals("id", result[0][0])
            assertEquals("3.0", result[0][2])
            assertEquals("3.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("12.0", result[0][5])
            assertEquals("4.0", result[0][6])
            assertEquals("N/A", result[0][7])
            assertEquals("N/A", result[0][8])
            assertEquals("SAMPLE" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])

            sql """analyze table stats_test2 with sync;"""
            result = sql """show column stats stats_test2(id);"""
            assertEquals(1, result.size())
            assertEquals("id", result[0][0])
            assertEquals("2.0", result[0][2])
            assertEquals("2.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("8.0", result[0][5])
            assertEquals("4.0", result[0][6])
            assertEquals("1", result[0][7])
            assertEquals("2", result[0][8])
            assertEquals("FULL" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])

            result = sql """show column stats stats_test2(value);"""
            assertEquals(1, result.size())
            assertEquals("value", result[0][0])
            assertEquals("2.0", result[0][2])
            assertEquals("2.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("2.0", result[0][5])
            assertEquals("1.0", result[0][6])
            assertEquals("\'*\'", result[0][7])
            assertEquals("\';\'", result[0][8])
            assertEquals("FULL" , result[0][9])
            assertEquals("FUNDAMENTALS" , result[0][10])
            assertEquals("MANUAL" , result[0][11])
            assertEquals("0" , result[0][12])


            sql """analyze table stats_test3 with sync"""
            result = sql """show column stats stats_test3(id);"""
            assertEquals(1, result.size())
            assertEquals("id", result[0][0])
            assertEquals("0.0", result[0][2])
            assertEquals("0.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("0.0", result[0][5])
            assertEquals("0.0", result[0][6])
            assertEquals("N/A", result[0][7])
            assertEquals("N/A", result[0][8])

            result = sql """show column stats stats_test3(value);"""
            assertEquals(1, result.size())
            assertEquals("value", result[0][0])
            assertEquals("0.0", result[0][2])
            assertEquals("0.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("0.0", result[0][5])
            assertEquals("0.0", result[0][6])
            assertEquals("N/A", result[0][7])
            assertEquals("N/A", result[0][8])


            // Test auto analyze policy
            sql """drop stats stats_test1"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("false", result[0][8])

            sql """ALTER CATALOG `${catalog_name}` SET PROPERTIES ('enable.auto.analyze'='true')"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("true", result[0][8])

            sql """ALTER CATALOG `${catalog_name}` SET PROPERTIES ('enable.auto.analyze'='false')"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("false", result[0][8])

            sql """analyze table stats_test1 PROPERTIES("use.auto.analyzer"="true")"""
            result = sql """show auto analyze stats_test1"""
            assertEquals(0, result.size())

            sql """ALTER TABLE stats_test1 SET ("auto_analyze_policy" = "enable");"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("true", result[0][8])

            sql """analyze table stats_test1 PROPERTIES("use.auto.analyzer"="true")"""
            result = sql """show auto analyze stats_test1"""
            assertEquals(1, result.size())

            sql """ALTER CATALOG `${catalog_name}` SET PROPERTIES ('enable.auto.analyze'='true')"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("true", result[0][8])

            sql """ALTER CATALOG `${catalog_name}` SET PROPERTIES ('enable.auto.analyze'='false')"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("true", result[0][8])

            sql """ALTER TABLE stats_test1 SET ("auto_analyze_policy" = "disable");"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("false", result[0][8])

            sql """ALTER CATALOG `${catalog_name}` SET PROPERTIES ('enable.auto.analyze'='true')"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("false", result[0][8])

            sql """ALTER CATALOG `${catalog_name}` SET PROPERTIES ('enable.auto.analyze'='false')"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("false", result[0][8])

            sql """ALTER TABLE stats_test1 SET ("auto_analyze_policy" = "base_on_catalog");"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("false", result[0][8])

            sql """ALTER CATALOG `${catalog_name}` SET PROPERTIES ('enable.auto.analyze'='true')"""
            result = sql """show table stats stats_test1"""
            assertEquals(1, result.size())
            assertEquals("true", result[0][8])

            sql """drop catalog if exists ${catalog_name}"""

        } finally {
        }
    }
}

