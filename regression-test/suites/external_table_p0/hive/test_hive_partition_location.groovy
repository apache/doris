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

suite("test_hive_partition_location", "p0,external,hive,external_docker,external_docker_hive") {
    def one_partition1 = """select * from partition_location_1 order by id;"""
    def one_partition2 = """select * from partition_location_1 where part='part1';"""
    def one_partition3 = """select * from partition_location_1 where part='part2';"""
    def one_partition4 = """select part from partition_location_1 where part='part1';"""
    def one_partition5 = """select part from partition_location_1 where part='part2';"""
    def one_partition6 = """select part from partition_location_1 order by part;"""

    def two_partition1 = """select * from partition_location_2 order by id;"""
    def two_partition2 = """select * from partition_location_2 where part1='part1_1';"""
    def two_partition3 = """select * from partition_location_2 where part2='part2_1';"""
    def two_partition4 = """select * from partition_location_2 where part1='part1_2';"""
    def two_partition5 = """select * from partition_location_2 where part2='part2_2';"""
    def two_partition6 = """select part1, part2 from partition_location_2 order by part1;"""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_partition_location"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use multi_catalog;"""
        qt_one_partition1 one_partition1
        qt_one_partition2 one_partition2
        qt_one_partition3 one_partition3
        qt_one_partition4 one_partition4
        qt_one_partition5 one_partition5
        qt_one_partition6 one_partition6

        qt_two_partition1 two_partition1
        qt_two_partition2 two_partition2
        qt_two_partition3 two_partition3
        qt_two_partition4 two_partition4
        qt_two_partition5 two_partition5
        qt_two_partition6 two_partition6
        sql """drop catalog if exists ${catalog_name};"""
    }
}

