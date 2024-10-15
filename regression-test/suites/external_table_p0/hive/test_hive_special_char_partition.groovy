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

suite("test_hive_special_char_partition", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_special_char_partition"
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
        qt_1 "select * from special_character_1_partition order by name"
        qt_2 "select name from special_character_1_partition where part='2023 01 01'"
        qt_3 "select name from special_character_1_partition where part='2023/01/01'"
        qt_4 "select * from special_character_1_partition where part='2023?01?01'"
        qt_5 "select * from special_character_1_partition where part='2023.01.01'"
        qt_6 "select * from special_character_1_partition where part='2023<01><01>'"
        qt_7 "select * from special_character_1_partition where part='2023:01:01'"
        qt_8 "select * from special_character_1_partition where part='2023=01=01'"
        qt_9 "select * from special_character_1_partition where part='2023\"01\"01'"
        qt_10 "select * from special_character_1_partition where part='2023\\'01\\'01'"
        qt_11 "select * from special_character_1_partition where part='2023\\\\01\\\\01'"
        qt_12 "select * from special_character_1_partition where part='2023%01%01'"
        qt_13 "select * from special_character_1_partition where part='2023#01#01'"
    }
}

