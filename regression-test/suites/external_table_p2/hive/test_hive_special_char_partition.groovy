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

suite("test_hive_special_char_partition", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_special_char_partition"
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

