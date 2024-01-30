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
    def one_partition6 = """select id, part1 from one_partition where part1 is null or part1>1 order by id;"""

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
    def two_partition18 = """select id, part1, part2 from two_partition where part2 = 'three' order by id;"""

    def string_part_prune1 = """select * from test_date_string_partition where cast(day1 as date) > cast("2023-08-16" as date);"""
    def string_part_prune2 = """select * from test_date_string_partition where cast(day1 as date) > cast("2023-08-16" as date) and day2="2023-08-16";"""
    def string_part_prune3 = """select * from test_date_string_partition where cast(day1 as date) > cast("2023-08-16" as date) and day2="2023-08-17";"""
    def string_part_prune4 = """select * from test_date_string_partition where cast(day1 as date) > cast("2023-08-16" as date) or day2<"2023-08-17";"""
    def string_part_prune5 = """select * from test_date_string_partition where cast(day1 as date) > cast("2023-08-16" as date) or cast(day2 as string) = "2023-08-17";"""
    def string_part_prune6 = """select * from test_date_string_partition where day1 in ("2023-08-16", "2023-08-15");"""
    def string_part_prune7 = """select * from test_date_string_partition where day1 in ("2023-08-16", "2023-08-18");"""
    def string_part_prune8 = """select * from test_date_string_partition where cast(day1 as date) in ("2023-08-16", "2023-08-18");"""
    def string_part_prune9 = """select * from test_date_string_partition where cast(day1 as date) in (cast("2023-08-16" as date), "2023-08-18");"""

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
        sql """set experimental_enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""
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
        qt_two_partition18 two_partition18

        order_qt_string_part_prune1 string_part_prune1
        order_qt_string_part_prune2 string_part_prune2
        order_qt_string_part_prune3 string_part_prune3
        order_qt_string_part_prune4 string_part_prune4
        order_qt_string_part_prune5 string_part_prune5
        order_qt_string_part_prune5 string_part_prune6
        order_qt_string_part_prune5 string_part_prune7
        order_qt_string_part_prune5 string_part_prune8
        order_qt_string_part_prune5 string_part_prune9

        explain {
            sql("${one_partition1}")
            contains "partition=3/3"
        }
        explain {
            sql("${one_partition2}")
            contains "partition=1/3"
        }
        explain {
            sql("${one_partition3}")
            contains "partition=3/3"
        }
        explain {
            sql("${one_partition4}")
            contains "partition=3/3"
        }
        explain {
            sql("${one_partition5}")
            contains "partition=3/3"
        }
        explain {
            sql("${one_partition6}")
            contains "partition=2/3"
        }

        explain {
            sql("${two_partition1}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition2}")
            contains "partition=3/4"
        }
        explain {
            sql("${two_partition3}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition4}")
            contains "partition=3/4"
        }
        explain {
            sql("${two_partition5}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition6}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition7}")
            contains "partition=3/4"
        }
        explain {
            sql("${two_partition8}")
            contains "partition=3/4"
        }
        explain {
            sql("${two_partition9}")
            contains "partition=3/4"
        }
        explain {
            sql("${two_partition10}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition11}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition12}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition13}")
            contains "partition=3/4"
        }
        explain {
            sql("${two_partition14}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition15}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition16}")
            contains "partition=4/4"
        }
        explain {
            sql("${two_partition17}")
            contains "partition=3/4"
        }
        explain {
            sql("${two_partition18}")
            contains "partition=4/4"
        }

        explain {
            sql("${string_part_prune1}")
            contains "partition=1/4"
        }
        explain {
            sql("${string_part_prune2}")
            contains "partition=0/4"
        }
        explain {
            sql("${string_part_prune3}")
            contains "partition=1/4"
        }
        explain {
            sql("${string_part_prune4}")
            contains "partition=4/4"
        }
        explain {
            sql("${string_part_prune5}")
            contains "partition=1/4"
        }
        explain {
            sql("${string_part_prune6}")
            contains "partition=2/4"
        }
        explain {
            sql("${string_part_prune7}")
            contains "partition=1/4"
        }
        explain {
            sql("${string_part_prune8}")
            contains "partition=1/4"
        }
        explain {
            sql("${string_part_prune9}")
            contains "partition=1/4"
        }
    }
}

