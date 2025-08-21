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

suite("test_drop_partition_range") {
    sql "DROP TABLE IF EXISTS drop_range_partition FORCE"

    // Test drop partitions by date range with DAY interval
    sql """
        CREATE TABLE IF NOT EXISTS drop_range_partition
        (
            `k1` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `date`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION `p_20240101` VALUES [("2024-01-01"), ("2024-01-05")),
            PARTITION `p_20240105` VALUES [("2024-01-05"), ("2024-01-09")),
            PARTITION `p_20240109` VALUES [("2024-01-09"), ("2024-01-13")),
            PARTITION `p_20240113` VALUES [("2024-01-13"), ("2024-01-17")),
            PARTITION `p_20240117` VALUES [("2024-01-17"), ("2024-01-21"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result1 = sql "show partitions from drop_range_partition;"
    assertEquals(result1.size(), 5)
    
    sql "ALTER TABLE drop_range_partition DROP PARTITION FROM ('2024-01-05') TO ('2024-01-17') INTERVAL 4 DAY;"
    List<List<Object>> result2 = sql "show partitions from drop_range_partition;"
    assertEquals(result2.size(), 2)
    
    sql "DROP TABLE IF EXISTS drop_range_partition FORCE"

    // Test drop partitions by date range with WEEK interval
    sql """
        CREATE TABLE `drop_range_partition` (
            `k1` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `k2` VARCHAR(20) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, `date`)
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
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result3 = sql "show partitions from drop_range_partition;"
    assertEquals(result3.size(), 9)
    
    sql "ALTER TABLE drop_range_partition DROP PARTITION FROM ('2024-01-08') TO ('2024-02-12') INTERVAL 1 WEEK;"
    List<List<Object>> result4 = sql "show partitions from drop_range_partition;"
    assertEquals(result4.size(), 4)
    
    sql "DROP TABLE IF EXISTS drop_range_partition FORCE"

    // Test drop partitions by numeric range
    sql """
        CREATE TABLE IF NOT EXISTS drop_range_partition
        (
            `k1` LARGEINT NOT NULL,
            `age` SMALLINT,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `age`)
        PARTITION BY RANGE(`age`)
        (
            FROM (1) TO (100) INTERVAL 10
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result5 = sql "show partitions from drop_range_partition;"
    assertEquals(result5.size(), 10)
    
    sql "ALTER TABLE drop_range_partition DROP PARTITION FROM (21) TO (81) INTERVAL 10;"
    List<List<Object>> result6 = sql "show partitions from drop_range_partition;"
    assertEquals(result6.size(), 4)
    
    sql "DROP TABLE IF EXISTS drop_range_partition FORCE"
}

