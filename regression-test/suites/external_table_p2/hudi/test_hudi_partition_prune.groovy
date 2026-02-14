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

suite("test_hudi_partition_prune", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_partition_prune"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hudiHmsPort = context.config.otherConfigs.get("hudiHmsPort")
    String hudiMinioPort = context.config.otherConfigs.get("hudiMinioPort")
    String hudiMinioAccessKey = context.config.otherConfigs.get("hudiMinioAccessKey")
    String hudiMinioSecretKey = context.config.otherConfigs.get("hudiMinioSecretKey")
    
    sql """drop catalog if exists ${catalog_name};"""
    
    for (String  use_hive_sync_partition : ['true','false']) {

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hudiHmsPort}',
                's3.endpoint' = 'http://${externalEnvIp}:${hudiMinioPort}',
                's3.access_key' = '${hudiMinioAccessKey}',
                's3.secret_key' = '${hudiMinioSecretKey}',
                's3.region' = 'us-east-1',
                'use_path_style' = 'true',
                'use_hive_sync_partition' = '${use_hive_sync_partition}'
            );
        """

        sql """ switch ${catalog_name};"""
        sql """ use regression_hudi;""" 
        sql """ set enable_fallback_to_original_planner=false """
        
        // Function to get commit timestamps dynamically from hudi_meta table function
        def getCommitTimestamps = { table_name ->
            def result = sql """ 
                SELECT timestamp 
                FROM hudi_meta("table"="${catalog_name}.regression_hudi.${table_name}", "query_type" = "timeline")
                WHERE action = 'commit' OR action = 'deltacommit'
                ORDER BY timestamp
            """
            return result.collect { it[0] }
        }
        
        // Get commit timestamps for two_partition_tb (used in time travel queries)
        def timestamps_two_partition = getCommitTimestamps("two_partition_tb")



        def one_partition_1_1 = """SELECT id,name,part1  FROM one_partition_tb WHERE part1 = 2024 ORDER BY id;"""
        def one_partition_2_1 = """SELECT id,name,part1  FROM one_partition_tb WHERE part1 = 2025 ORDER BY id;"""
        def one_partition_3_all = """SELECT id,name,part1  FROM one_partition_tb ORDER BY id;"""
        def one_partition_4_all = """SELECT id,name,part1  FROM one_partition_tb WHERE id = 5 ORDER BY id;"""
        def one_partition_5_1 = """SELECT id,name,part1  FROM one_partition_tb WHERE part1 = 2024 AND id >= 3 ORDER BY id;"""

        def two_partition_1_1 = """SELECT id,name,part1,part2  FROM two_partition_tb WHERE part1 = 'US' AND part2 = 1 ORDER BY id;"""
        def two_partition_2_1 = """SELECT id,name,part1,part2  FROM two_partition_tb WHERE part1 = 'EU' AND part2 = 2 ORDER BY id;"""
        def two_partition_3_2 = """SELECT id,name,part1,part2  FROM two_partition_tb WHERE part1 = 'US' ORDER BY id;"""
        def two_partition_4_all = """SELECT id,name,part1,part2  FROM two_partition_tb ORDER BY id;"""
        def two_partition_5_1 = """SELECT id,name,part1,part2  FROM two_partition_tb WHERE part1 = 'US' AND part2 = 2 AND id > 5 ORDER BY id;"""
        def two_partition_6_1 = """SELECT id,name,part1,part2  FROM two_partition_tb WHERE part1 = 'EU' AND part2 = 2 ORDER BY id;"""

        def three_partition_1_1 = """SELECT id,name,part1,part2,part3  FROM three_partition_tb WHERE part1 = 'US' AND part2 = 2024 AND part3 = 'Q1' ORDER BY id;"""
        def three_partition_2_1 = """SELECT id,name,part1,part2,part3  FROM three_partition_tb WHERE part1 = 'EU' AND part2 = 2025 AND part3 = 'Q2' ORDER BY id;"""
        def three_partition_3_3 = """SELECT id,name,part1,part2,part3  FROM three_partition_tb WHERE part1 = 'AS' AND part2 = 2025 ORDER BY id;"""
        def three_partition_4_2 = """SELECT id,name,part1,part2,part3  FROM three_partition_tb WHERE part1 = 'US' AND part3 = 'Q1' ORDER BY id;"""
        def three_partition_5_all = """SELECT id,name,part1,part2,part3  FROM three_partition_tb ORDER BY id;"""
        def three_partition_6_1 = """SELECT id,name,part1,part2,part3  FROM three_partition_tb WHERE part1 = 'EU' AND part2 = 2024 AND part3 = 'Q1' ORDER BY id;"""
        def three_partition_7_7 = """SELECT id,name,part1,part2,part3  FROM three_partition_tb WHERE part2 = 2025 ORDER BY id;"""
        def three_partition_8_2 = """SELECT id,name,part1,part2,part3  FROM three_partition_tb WHERE part1 = 'US' AND part3 = 'Q2' AND id BETWEEN 6 AND 10 ORDER BY id;"""

        def one_partition_boolean = """SELECT id,name,part1  FROM boolean_partition_tb WHERE part1 = true ORDER BY id;"""
        def one_partition_tinyint = """SELECT id,name,part1  FROM tinyint_partition_tb WHERE part1 = 1 ORDER BY id;"""
        def one_partition_smallint = """SELECT id,name,part1  FROM smallint_partition_tb WHERE part1 = 10 ORDER BY id;"""
        def one_partition_int = """SELECT id,name,part1  FROM int_partition_tb WHERE part1 = 100 ORDER BY id;"""
        def one_partition_bigint = """SELECT id,name,part1  FROM bigint_partition_tb WHERE part1 = 1234567890 ORDER BY id;"""
        def one_partition_string = """SELECT id,name,part1  FROM string_partition_tb WHERE part1 = 'RegionA' ORDER BY id;"""
        def one_partition_date = """SELECT id,name,part1  FROM date_partition_tb WHERE part1 = '2023-12-01' ORDER BY id;"""
        def one_partition_timestamp = """SELECT id,name,part1  FROM timestamp_partition_tb WHERE part1 = '2023-12-01 08:00:00' ORDER BY id;"""



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
        def one_partition_6_0 = """SELECT id,name,part1  FROM one_partition_tb WHERE part1 = 2023 ORDER BY id;"""
        qt_one_partition_6_0 one_partition_6_0
        explain {
            sql("${one_partition_6_0}")
            contains "partition=0/2"
        }

        def two_partition_7_0 = """SELECT id,name,part1  FROM two_partition_tb WHERE part1 = 'CN' AND part2 = 1 ORDER BY id;"""
        qt_two_partition_7_0 two_partition_7_0
        explain {
            sql("${two_partition_7_0}")
            contains "partition=0/4"
        }

        def two_partition_8_0 = """SELECT id,name,part1  FROM two_partition_tb WHERE part1 = 'US' AND part2 = 3 ORDER BY id;"""
        qt_two_partition_8_0 two_partition_8_0
        explain {
            sql("${two_partition_8_0}")
            contains "partition=0/4"
        }

        def three_partition_9_0 = """SELECT id,name,part1  FROM three_partition_tb WHERE part1 = 'US' AND part2 = 2023 AND part3 = 'Q1' ORDER BY id;"""
        qt_three_partition_9_0 three_partition_9_0
        explain {
            sql("${three_partition_9_0}")
            contains "partition=0/10"
        }

        def three_partition_10_0 = """SELECT id,name,part1  FROM three_partition_tb WHERE part1 = 'EU' AND part2 = 2024 AND part3 = 'Q4' ORDER BY id;"""
        qt_three_partition_10_0 three_partition_10_0
        explain {
            sql("${three_partition_10_0}")
            contains "partition=0/10"
        }

        def three_partition_11_0 = """SELECT id,name,part1  FROM three_partition_tb WHERE part1 = 'AS' AND part2 = 2025 AND part3 = 'Q4' ORDER BY id;"""
        qt_three_partition_11_0 three_partition_11_0
        explain {
            sql("${three_partition_11_0}")
            contains "partition=0/10"
        }


        //time travel - use dynamic commit timestamps
        // Note: two_partition_tb has 10 INSERT statements, creating 10 commits
        // Final partitions: (US,1), (US,2), (EU,1), (EU,2) = 4 partitions total
        if (timestamps_two_partition.size() >= 10) {
            // Use the last commit timestamp (all 10 records, 4 partitions)
            def last_commit = timestamps_two_partition[9]
            def time_travel_two_partition_1_4 = "select id,name,part1,part2 from two_partition_tb FOR TIME AS OF '${last_commit}' order by id;"
            def time_travel_two_partition_2_2 = "select id,name,part1,part2 from two_partition_tb FOR TIME AS OF '${last_commit}' where part1='US' order by id;"
            def time_travel_two_partition_3_2 = "select id,name,part1,part2 from two_partition_tb FOR TIME AS OF '${last_commit}' where part2=2 order by id;"
            def time_travel_two_partition_4_0 = "select id,name,part1,part2 from two_partition_tb FOR TIME AS OF '${last_commit}' where part2=10 order by id;"

            qt_time_travel_two_partition_1_4 time_travel_two_partition_1_4
            explain {
                sql("${time_travel_two_partition_1_4}")
                contains "partition=4/4"
            }

            qt_time_travel_two_partition_2_2 time_travel_two_partition_2_2
            explain {
                sql("${time_travel_two_partition_2_2}")
                contains "partition=2/4"
            }

            qt_time_travel_two_partition_3_2 time_travel_two_partition_3_2
            explain {
                sql("${time_travel_two_partition_3_2}")
                contains "partition=2/4"
            }

            qt_time_travel_two_partition_4_0 time_travel_two_partition_4_0
            explain {
                sql("${time_travel_two_partition_4_0}")
                contains "partition=0/4"
            }

            // Use the first commit (after first INSERT: 1 record in partition US,1)
            def first_commit = timestamps_two_partition[0]
            def time_travel_two_partition_5_1 = "select id,name,part1,part2 from two_partition_tb FOR TIME AS OF '${first_commit}' order by id;"
            qt_time_travel_two_partition_5_1 time_travel_two_partition_5_1
            explain {
                sql("${time_travel_two_partition_5_1}")
                // First commit should have 1 record in partition (US, part2=1)
                contains "partition=1/1"
            }

            // Use a middle commit (after 3 inserts: 3 records in partition US,1)
            def middle_commit = timestamps_two_partition[2]
            def time_travel_two_partition_6_1 = "select id,name,part1,part2 from two_partition_tb FOR TIME AS OF '${middle_commit}' order by id;"
            qt_time_travel_two_partition_6_1 time_travel_two_partition_6_1
            explain {
                sql("${time_travel_two_partition_6_1}")
                // After 3 inserts, should have 1 partition (US, part2=1) with 3 records
                contains "partition=1/1"
            }
        }

        // all types as partition
        qt_one_partition_boolean one_partition_boolean
        explain {
            sql("${one_partition_boolean}")
            contains "partition=1/2"
        }
        qt_one_partition_tinyint one_partition_tinyint
        explain {
            sql("${one_partition_tinyint}")
            contains "partition=1/2"
        }
        qt_one_partition_smallint one_partition_smallint
        explain {
            sql("${one_partition_smallint}")
            contains "partition=1/2"
        }
        qt_one_partition_int one_partition_int
        explain {
            sql("${one_partition_int}")
            contains "partition=1/2"
        }
        qt_one_partition_bigint one_partition_bigint
        explain {
            sql("${one_partition_bigint}")
            contains "partition=1/2"
        }
        qt_one_partition_string one_partition_string
        explain {
            sql("${one_partition_string}")
            contains "partition=1/2"
        }
        qt_one_partition_date one_partition_date
        explain {
            sql("${one_partition_date}")
            contains "partition=1/2"
        }
        qt_one_partition_timestamp one_partition_timestamp
        explain {
            sql("${one_partition_timestamp}")
            contains "partition=1/2"
        }

        sql """drop catalog if exists ${catalog_name};"""


    }
    
}