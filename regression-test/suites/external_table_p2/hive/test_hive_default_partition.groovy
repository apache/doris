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

suite("test_hive_default_partition", "p2") {
    def one_partition1 = """select * from one_partition order by id;"""
    def one_partition2 = """select id, part1 from one_partition where part1 is null order by id;"""
    def one_partition3 = """select id from one_partition where part1 is not null order by id;"""
    def one_partition4 = """select part1 from one_partition where part1>0 order by id;"""
    def one_partition5 = """select id, part1 from one_partition where part1 is null or id>3 order by id;"""

    def two_partition1 = """select * from two_partition order by id;"""
    def two_partition2 = """select id, part1, part2 from two_partition where part1 is null order by id;"""
    def two_partition3 = """select id, part1, part2 from two_partition where part1 is not null order by id;"""
    def two_partition4 = """select id, part1, part2 from two_partition where part2 is null order by id;"""
    def two_partition5 = """select id, part1, part2 from two_partition where part2 is not null order by id;"""
    def two_partition6 = """select id, part1, part2 from two_partition where part1 is not null and part2 is not null order by id;"""
    def two_partition7 = """select id, part1, part2 from two_partition where part1 is null and part2 is not null order by id;"""
    def two_partition8 = """select id, part1, part2 from two_partition where part1 is not null and part2 is null order by id;"""
    def two_partition9 = """select id, part1, part2 from two_partition where part1 is null and part2 is null order by id;"""
    def two_partition10 = """select id, part1, part2 from two_partition where part1 is not null or part2 is not null order by id;"""
    def two_partition11 = """select id, part1, part2 from two_partition where part1 is null or part2 is not null order by id;"""
    def two_partition12 = """select id, part1, part2 from two_partition where part1 is not null or part2 is null order by id;"""
    def two_partition13 = """select id, part1, part2 from two_partition where part1 is null or part2 is null order by id;"""
    def two_partition14 = """select id, part1, part2 from two_partition where part1 is not null or part2 is not null order by id;"""
    def two_partition15 = """select id, part1, part2 from two_partition where id > 5 order by id;"""
    def two_partition16 = """select id, part1, part2 from two_partition where part1>0 order by id;"""
    def two_partition17 = """select id, part1, part2 from two_partition where part2 = 'one' order by id;"""

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "hive_default_partition"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
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

        qt_two_partition1 two_partition1
        qt_two_partition2 two_partition2
        qt_two_partition3 two_partition3
        qt_two_partition4 two_partition4
        qt_two_partition5 two_partition5
        qt_two_partition6 two_partition6
        qt_two_partition7 two_partition7
        qt_two_partition8 two_partition8
        qt_two_partition9 two_partition9
        qt_two_partition10 two_partition10
        qt_two_partition11 two_partition11
        qt_two_partition12 two_partition12
        qt_two_partition13 two_partition13
        qt_two_partition14 two_partition14
        qt_two_partition15 two_partition15
        qt_two_partition16 two_partition16
        qt_two_partition17 two_partition17

    }
}

