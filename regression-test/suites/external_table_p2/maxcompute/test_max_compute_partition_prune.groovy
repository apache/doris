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



/*
CREATE TABLE one_partition_tb (
    id INT,
    name string
)
PARTITIONED BY (part1 INT);
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (1, 'Alice');
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (2, 'Bob');
INSERT INTO one_partition_tb PARTITION (part1=2024) VALUES (3, 'Charlie');
INSERT INTO one_partition_tb PARTITION (part1=2025) VALUES (4, 'David');
INSERT INTO one_partition_tb PARTITION (part1=2025) VALUES (5, 'Eva');
CREATE TABLE two_partition_tb (
    id INT,
    name string
)
PARTITIONED BY (part1 STRING, part2 int);
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (1, 'Alice');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (2, 'Bob');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=1) VALUES (3, 'Charlie');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=2) VALUES (4, 'David');
INSERT INTO two_partition_tb PARTITION (part1='US', part2=2) VALUES (5, 'Eva');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=1) VALUES (6, 'Frank');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=1) VALUES (7, 'Grace');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (8, 'Hannah');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (9, 'Ivy');
INSERT INTO two_partition_tb PARTITION (part1='EU', part2=2) VALUES (10, 'Jack');
CREATE TABLE three_partition_tb (
    id INT,
    name string
)
PARTITIONED BY (part1 STRING, part2 INT, part3 STRING);
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (1, 'Alice');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (2, 'Bob');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q1') VALUES (3, 'Charlie');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q2') VALUES (4, 'David');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2024, part3='Q2') VALUES (5, 'Eva');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2025, part3='Q1') VALUES (6, 'Frank');
INSERT INTO three_partition_tb PARTITION (part1='US', part2=2025, part3='Q2') VALUES (7, 'Grace');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2024, part3='Q1') VALUES (8, 'Hannah');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2024, part3='Q1') VALUES (9, 'Ivy');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q2') VALUES (10, 'Jack');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q2') VALUES (11, 'Leo');
INSERT INTO three_partition_tb PARTITION (part1='EU', part2=2025, part3='Q3') VALUES (12, 'Mia');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q1') VALUES (13, 'Nina');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q2') VALUES (14, 'Oscar');
INSERT INTO three_partition_tb PARTITION (part1='AS', part2=2025, part3='Q3') VALUES (15, 'Paul');
select * from one_partition_tb;
select * from two_partition_tb;
select * from three_partition_tb;
show partitions one_partition_tb;
show partitions two_partition_tb;
show partitions three_partition_tb;
*/

suite("test_max_compute_partition_prune", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {


    def one_partition_1_1 = """SELECT * FROM one_partition_tb WHERE part1 = 2024 ORDER BY id;"""
    def one_partition_2_1 = """SELECT * FROM one_partition_tb WHERE part1 = 2025 ORDER BY id;"""
    def one_partition_3_all = """SELECT * FROM one_partition_tb ORDER BY id;"""
    def one_partition_4_all = """SELECT * FROM one_partition_tb WHERE id = 5 ORDER BY id;"""
    def one_partition_5_1 = """SELECT * FROM one_partition_tb WHERE part1 = 2024 AND id >= 3 ORDER BY id;"""

    def two_partition_1_1 = """SELECT * FROM two_partition_tb WHERE part1 = 'US' AND part2 = 1 ORDER BY id;"""
    def two_partition_2_1 = """SELECT * FROM two_partition_tb WHERE part1 = 'EU' AND part2 = 2 ORDER BY id;"""
    def two_partition_3_2 = """SELECT * FROM two_partition_tb WHERE part1 = 'US' ORDER BY id;"""
    def two_partition_4_all = """SELECT * FROM two_partition_tb ORDER BY id;"""
    def two_partition_5_1 = """SELECT * FROM two_partition_tb WHERE part1 = 'US' AND part2 = 2 AND id > 5 ORDER BY id;"""
    def two_partition_6_1 = """SELECT * FROM two_partition_tb WHERE part1 = 'EU' AND part2 = 2 ORDER BY id;"""

    def three_partition_1_1 = """SELECT * FROM three_partition_tb WHERE part1 = 'US' AND part2 = 2024 AND part3 = 'Q1' ORDER BY id;"""
    def three_partition_2_1 = """SELECT * FROM three_partition_tb WHERE part1 = 'EU' AND part2 = 2025 AND part3 = 'Q2' ORDER BY id;"""
    def three_partition_3_3 = """SELECT * FROM three_partition_tb WHERE part1 = 'AS' AND part2 = 2025 ORDER BY id;"""
    def three_partition_4_2 = """SELECT * FROM three_partition_tb WHERE part1 = 'US' AND part3 = 'Q1' ORDER BY id;"""
    def three_partition_5_all = """SELECT * FROM three_partition_tb ORDER BY id;"""
    def three_partition_6_1 = """SELECT * FROM three_partition_tb WHERE part1 = 'EU' AND part2 = 2024 AND part3 = 'Q1' ORDER BY id;"""
    def three_partition_7_7 = """SELECT * FROM three_partition_tb WHERE part2 = 2025 ORDER BY id;"""
    def three_partition_8_2 = """SELECT * FROM three_partition_tb WHERE part1 = 'US' AND part3 = 'Q2' AND id BETWEEN 6 AND 10 ORDER BY id;"""


    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("ak")
        String sk = context.config.otherConfigs.get("sk");
        String mc_db = "mc_datalake"
        String mc_catalog_name = "test_max_compute_partition_prune"


        for (String  enable_profile : ["true","false"] ) {
            sql """set enable_profile = ${enable_profile} """;

            for (String num_partitions : ["1","10","100"] ) {
                sql "set num_partitions_in_batch_mode =  ${num_partitions} "

                for (String cross_partition : ["true","false"] ) {

                    sql """drop catalog if exists ${mc_catalog_name};"""
                    sql """
                        create catalog if not exists ${mc_catalog_name} properties (
                            "type" = "max_compute",
                            "mc.default.project" = "${mc_db}",
                            "mc.access_key" = "${ak}",
                            "mc.secret_key" = "${sk}",
                            "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                            "mc.split_cross_partition" = "${cross_partition}"
                        );
                    """
                    sql """ switch ${mc_catalog_name} """
                    sql """ use ${mc_db}"""

                    qt_one_partition_1_1 one_partition_1_1
                    explain {
                        sql("${one_partition_1_1}")
                        contains "partition=1/2"
                    }

                    qt_one_partition_2_1 one_partition_2_1
                    explain {
                        sql("${one_partition_2_1}")
                        contains "partition=1/2"
                    }

                    qt_one_partition_3_all one_partition_3_all
                    explain {
                        sql("${one_partition_3_all}")
                        contains "partition=2/2"
                    }

                    qt_one_partition_4_all one_partition_4_all
                    explain {
                        sql("${one_partition_4_all}")
                        contains "partition=2/2"
                    }

                    qt_one_partition_5_1 one_partition_5_1
                    explain {
                        sql("${one_partition_5_1}")
                        contains "partition=1/2"
                    }


                    qt_two_partition_1_1 two_partition_1_1
                    explain {
                        sql("${two_partition_1_1}")
                        contains "partition=1/4"
                    }

                    qt_two_partition_2_1 two_partition_2_1
                    explain {
                        sql("${two_partition_2_1}")
                        contains "partition=1/4"
                    }

                    qt_two_partition_3_2 two_partition_3_2
                    explain {
                        sql("${two_partition_3_2}")
                        contains "partition=2/4"
                    }

                    qt_two_partition_4_all two_partition_4_all
                    explain {
                        sql("${two_partition_4_all}")
                        contains "partition=4/4"
                    }

                    qt_two_partition_5_1 two_partition_5_1
                    explain {
                        sql("${two_partition_5_1}")
                        contains "partition=1/4"
                    }

                    qt_two_partition_6_1 two_partition_6_1
                    explain {
                        sql("${two_partition_6_1}")
                        contains "partition=1/4"
                    }



                    qt_three_partition_1_1 three_partition_1_1
                    explain {
                        sql("${three_partition_1_1}")
                        contains "partition=1/10"
                    }

                    qt_three_partition_2_1 three_partition_2_1
                    explain {
                        sql("${three_partition_2_1}")
                        contains "partition=1/10"
                    }

                    qt_three_partition_3_3 three_partition_3_3
                    explain {
                        sql("${three_partition_3_3}")
                        contains "partition=3/10"
                    }

                    qt_three_partition_4_2 three_partition_4_2
                    explain {
                        sql("${three_partition_4_2}")
                        contains "partition=2/10"
                    }

                    qt_three_partition_5_all three_partition_5_all
                    explain {
                        sql("${three_partition_5_all}")
                        contains "partition=10/10"
                    }

                    qt_three_partition_6_1 three_partition_6_1
                    explain {
                        sql("${three_partition_6_1}")
                        contains "partition=1/10"
                    }

                    qt_three_partition_7_7 three_partition_7_7
                    explain {
                        sql("${three_partition_7_7}")
                        contains "partition=7/10"
                    }

                    qt_three_partition_8_2 three_partition_8_2
                    explain {
                        sql("${three_partition_8_2}")
                        contains "partition=2/10"
                    }


                    // 0 partitions
                    def one_partition_6_0 = """SELECT * FROM one_partition_tb WHERE part1 = 2023 ORDER BY id;"""
                    qt_one_partition_6_0 one_partition_6_0
                    explain {
                        sql("${one_partition_6_0}")
                        contains "partition=0/2"
                    }

                    def two_partition_7_0 = """SELECT * FROM two_partition_tb WHERE part1 = 'CN' AND part2 = 1 ORDER BY id;"""
                    qt_two_partition_7_0 two_partition_7_0
                    explain {
                        sql("${two_partition_7_0}")
                        contains "partition=0/4"
                    }

                    def two_partition_8_0 = """SELECT * FROM two_partition_tb WHERE part1 = 'US' AND part2 = 3 ORDER BY id;"""
                    qt_two_partition_8_0 two_partition_8_0
                    explain {
                        sql("${two_partition_8_0}")
                        contains "partition=0/4"
                    }

                    def three_partition_9_0 = """SELECT * FROM three_partition_tb WHERE part1 = 'US' AND part2 = 2023 AND part3 = 'Q1' ORDER BY id;"""
                    qt_three_partition_9_0 three_partition_9_0
                    explain {
                        sql("${three_partition_9_0}")
                        contains "partition=0/10"
                    }

                    def three_partition_10_0 = """SELECT * FROM three_partition_tb WHERE part1 = 'EU' AND part2 = 2024 AND part3 = 'Q4' ORDER BY id;"""
                    qt_three_partition_10_0 three_partition_10_0
                    explain {
                        sql("${three_partition_10_0}")
                        contains "partition=0/10"
                    }

                    def three_partition_11_0 = """SELECT * FROM three_partition_tb WHERE part1 = 'AS' AND part2 = 2025 AND part3 = 'Q4' ORDER BY id;"""
                    qt_three_partition_11_0 three_partition_11_0
                    explain {
                        sql("${three_partition_11_0}")
                        contains "partition=0/10"
                    }
                }
            }
        }
    }
}