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

suite("test_alter_add_multi_partition") {
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"
    sql """
        CREATE TABLE IF NOT EXISTS add_multi_partition
        (
            `user_id` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `city` VARCHAR(20),
            `age` SMALLINT,
            `max_dwell_time` INT MAX DEFAULT "0",
            `min_dwell_time` INT MIN DEFAULT "99999"
        )
        ENGINE=OLAP
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01"))
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result1  = sql "show partitions from add_multi_partition;"
    assertEquals(result1.size(), 1)
    sql "ALTER TABLE add_multi_partition ADD PARTITIONS FROM ('2019-01-01') TO ('2025-01-01') INTERVAL 1 YEAR;"
    List<List<Object>> result2  = sql "show partitions from add_multi_partition;"
    assertEquals(result2.size(), 7)
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"

    
    sql """
        CREATE TABLE `add_multi_partition` (
            `user_id` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `city` VARCHAR(20) NULL,
            `age` SMALLINT NULL,
            `max_dwell_time` INT MAX NULL DEFAULT "0",
            `min_dwell_time` INT MIN NULL DEFAULT "99999"
        ) ENGINE=OLAP
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`)
        PARTITION BY RANGE(`date`)
        (PARTITION p_2024_01 VALUES [('2024-01-01'), ('2024-01-08')),
        PARTITION p_2024_02 VALUES [('2024-01-08'), ('2024-01-15')),
        PARTITION p_2024_03 VALUES [('2024-01-15'), ('2024-01-22')),
        PARTITION p_2024_04 VALUES [('2024-01-22'), ('2024-01-29')),
        PARTITION p_2024_05 VALUES [('2024-01-29'), ('2024-02-05')),
        PARTITION p_2024_06 VALUES [('2024-02-05'), ('2024-02-12')),
        PARTITION p_2024_07 VALUES [('2024-02-12'), ('2024-02-19')),
        PARTITION p_2024_08 VALUES [('2024-02-19'), ('2024-02-26')),
        PARTITION p_2024_09 VALUES [('2024-02-26'), ('2024-03-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result3  = sql "show partitions from add_multi_partition;"
    assertEquals(result3.size(), 9)
    sql "ALTER TABLE add_multi_partition ADD PARTITIONS FROM ('2024-04-01') TO ('2025-01-01') INTERVAL 1 WEEK;"
    List<List<Object>> result4  = sql "show partitions from add_multi_partition;"
    assertEquals(result4.size(), 49)
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"


    sql """
        CREATE TABLE `add_multi_partition` (
        `user_id` LARGEINT NOT NULL,
        `date` DATE NOT NULL,
        `city` VARCHAR(20) NULL,
        `age` SMALLINT NULL,
        `max_dwell_time` INT MAX NULL DEFAULT "0",
        `min_dwell_time` INT MIN NULL DEFAULT "99999"
        ) ENGINE=OLAP
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`)
        PARTITION BY RANGE(`date`)
        (PARTITION p_20240101 VALUES [('2024-01-01'), ('2024-01-02')),
        PARTITION p_20240102 VALUES [('2024-01-02'), ('2024-01-03')),
        PARTITION p_20240103 VALUES [('2024-01-03'), ('2024-01-04')),
        PARTITION p_20240104 VALUES [('2024-01-04'), ('2024-01-05')),
        PARTITION p_20240105 VALUES [('2024-01-05'), ('2024-01-06')),
        PARTITION p_20240106 VALUES [('2024-01-06'), ('2024-01-07')),
        PARTITION p_20240107 VALUES [('2024-01-07'), ('2024-01-08')),
        PARTITION p_20240108 VALUES [('2024-01-08'), ('2024-01-09')),
        PARTITION p_20240109 VALUES [('2024-01-09'), ('2024-01-10')),
        PARTITION p_20240110 VALUES [('2024-01-10'), ('2024-01-11')),
        PARTITION p_20240111 VALUES [('2024-01-11'), ('2024-01-12')),
        PARTITION p_20240112 VALUES [('2024-01-12'), ('2024-01-13')),
        PARTITION p_20240113 VALUES [('2024-01-13'), ('2024-01-14')),
        PARTITION p_20240114 VALUES [('2024-01-14'), ('2024-01-15')),
        PARTITION p_20240115 VALUES [('2024-01-15'), ('2024-01-16')),
        PARTITION p_20240116 VALUES [('2024-01-16'), ('2024-01-17')),
        PARTITION p_20240117 VALUES [('2024-01-17'), ('2024-01-18')),
        PARTITION p_20240118 VALUES [('2024-01-18'), ('2024-01-19')),
        PARTITION p_20240119 VALUES [('2024-01-19'), ('2024-01-20')),
        PARTITION p_20240120 VALUES [('2024-01-20'), ('2024-01-21')),
        PARTITION p_20240121 VALUES [('2024-01-21'), ('2024-01-22')),
        PARTITION p_20240122 VALUES [('2024-01-22'), ('2024-01-23')),
        PARTITION p_20240123 VALUES [('2024-01-23'), ('2024-01-24')),
        PARTITION p_20240124 VALUES [('2024-01-24'), ('2024-01-25')),
        PARTITION p_20240125 VALUES [('2024-01-25'), ('2024-01-26')),
        PARTITION p_20240126 VALUES [('2024-01-26'), ('2024-01-27')),
        PARTITION p_20240127 VALUES [('2024-01-27'), ('2024-01-28')),
        PARTITION p_20240128 VALUES [('2024-01-28'), ('2024-01-29')),
        PARTITION p_20240129 VALUES [('2024-01-29'), ('2024-01-30')),
        PARTITION p_20240130 VALUES [('2024-01-30'), ('2024-01-31')),
        PARTITION p_20240131 VALUES [('2024-01-31'), ('2024-02-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result5  = sql "show partitions from add_multi_partition;"
    assertEquals(result5.size(), 31)
    sql "ALTER TABLE add_multi_partition ADD PARTITIONS FROM ('2024-02-01') TO ('2024-03-01') INTERVAL 1 DAY;"
    List<List<Object>> result6  = sql "show partitions from add_multi_partition;"
    assertEquals(result6.size(), 60)
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"

    
    sql """
        CREATE TABLE IF NOT EXISTS add_multi_partition
        (
            `user_id` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `city` VARCHAR(20),
            `age` SMALLINT,
            `max_dwell_time` INT MAX DEFAULT "0",
            `min_dwell_time` INT MIN DEFAULT "99999"
        )
        ENGINE=OLAP
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`)
        PARTITION BY RANGE(`age`)
        (
            FROM (1) TO (100) INTERVAL 10
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result7  = sql "show partitions from add_multi_partition;"
    assertEquals(result7.size(), 10)
    sql "ALTER TABLE add_multi_partition ADD PARTITIONS FROM (100) TO (200) INTERVAL 10;"
    List<List<Object>> result8  = sql "show partitions from add_multi_partition;"
    assertEquals(result8.size(), 20)
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"

}