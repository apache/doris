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

suite("test_wide_table", "p0,external,hive,external_docker,external_docker_hive") {

    def formats = ["_orc"]
    def decimal_test1 = """select col1, col70, col71, col81, col100, col534 from wide_table1SUFFIX where col1 is not null order by col1 limit 1;"""
    def decimal_test2 = """select * from
     (select col1, col70, col71, col81, col100, col534 from wide_table1SUFFIX where col1 is not null order by col1 limit 1) as T where col100 = 9988065.8366;
         """
    def decimal_test3 = """select * from
     (select col1, col70, col71, col81, col100, col534 from wide_table1SUFFIX where col1 is not null order by col1 limit 1) as T  where col100 = 9988065.8367;
         """
    def decimal_test4 = """select * from
     (select col1, col70, col71, col81, col100, col534 from wide_table1SUFFIX where col1 is not null order by col1 limit 1) as T  where col100 = 9988065.836;
         """
    def decimal_test5 = """select * from
     (select col1, col70, col71, col81, col100, col534 from wide_table1SUFFIX where col1 is not null order by col1 limit 1) as T  where col100 = 9988065.836600;
     """
    def decimal_test6 = """select * from
     (select col1, col70, col71, col81, col100, col534 from wide_table1SUFFIX where col1 is not null order by col1 limit 1) as T  where col100 > 9988065.83653;
     """
    def decimal_test7 = """select * from
     (select col1, col70, col71, col81, col100, col534 from wide_table1SUFFIX where col1 is not null order by col1 limit 1) as T  where col100 < 9988065.83673;
     """
    def decimal_test8 = """select max(col1), max(col70), max(col71), min(col81), min(col100), min(col534) from wide_table1SUFFIX;"""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_complex_types"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use multi_catalog;"""
        logger.info("use multi_catalog")

        for (String format in formats) {
            logger.info("Process format " + format)
            qt_01 decimal_test1.replace("SUFFIX", format)
            qt_02 decimal_test2.replace("SUFFIX", format)
            qt_03 decimal_test3.replace("SUFFIX", format)
            qt_04 decimal_test4.replace("SUFFIX", format)
            qt_05 decimal_test5.replace("SUFFIX", format)
            qt_06 decimal_test6.replace("SUFFIX", format)
            qt_07 decimal_test7.replace("SUFFIX", format)
            qt_08 decimal_test8.replace("SUFFIX", format)
        }
    }
}

