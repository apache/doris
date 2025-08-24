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
    
    // Test 1: Drop partitions by date range with DAY interval
    sql "DROP TABLE IF EXISTS drop_range_partition_1 FORCE"
    sql """
        CREATE TABLE drop_range_partition_1
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
        PROPERTIES ("replication_num" = "1");
    """
    List<List<Object>> result1 = sql "show partitions from drop_range_partition_1;"
    assertEquals(result1.size(), 5)
    
    sql "ALTER TABLE drop_range_partition_1 DROP PARTITION FROM ('2024-01-05') TO ('2024-01-17') INTERVAL 4 DAY;"
    List<List<Object>> result2 = sql "show partitions from drop_range_partition_1;"
    assertEquals(result2.size(), 2)

    // Test 2: Drop partitions by date range with WEEK interval
    sql "DROP TABLE IF EXISTS drop_range_partition_2 FORCE"
    sql """
        CREATE TABLE drop_range_partition_2 (
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
        PROPERTIES ("replication_num" = "1");
    """
    List<List<Object>> result3 = sql "show partitions from drop_range_partition_2;"
    assertEquals(result3.size(), 9)
    
    sql "ALTER TABLE drop_range_partition_2 DROP PARTITION FROM ('2024-01-08') TO ('2024-02-12') INTERVAL 1 WEEK;"
    List<List<Object>> result4 = sql "show partitions from drop_range_partition_2;"
    assertEquals(result4.size(), 4)

    // Test 3: Drop partitions by numeric range
    sql "DROP TABLE IF EXISTS drop_range_partition_3 FORCE"
    sql """
        CREATE TABLE drop_range_partition_3
        (
            `k1` LARGEINT NOT NULL,
            `age` SMALLINT,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `age`)
        PARTITION BY RANGE(`age`)
        (FROM (1) TO (100) INTERVAL 10)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    List<List<Object>> result5 = sql "show partitions from drop_range_partition_3;"
    assertEquals(result5.size(), 10)
    
    sql "ALTER TABLE drop_range_partition_3 DROP PARTITION FROM (1) TO (100) INTERVAL 10;"
    List<List<Object>> result6 = sql "show partitions from drop_range_partition_3;"
    assertEquals(result6.size(), 0)

    // Test 4: Drop partitions with HOUR interval
    sql "DROP TABLE IF EXISTS drop_range_partition_4 FORCE"
    sql """
        CREATE TABLE drop_range_partition_4
        (
            `k1` LARGEINT NOT NULL,
            `dt` DATETIME NOT NULL,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `dt`)
        PARTITION BY RANGE(`dt`)
        (FROM ("2024-01-01 00:00:00") TO ("2024-01-01 12:00:00") INTERVAL 2 HOUR)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    List<List<Object>> result7 = sql "show partitions from drop_range_partition_4;"
    assertEquals(result7.size(), 6)
    
    sql "ALTER TABLE drop_range_partition_4 DROP PARTITION FROM ('2024-01-01 02:00:00') TO ('2024-01-01 08:00:00') INTERVAL 2 HOUR;"
    List<List<Object>> result8 = sql "show partitions from drop_range_partition_4;"
    assertEquals(result8.size(), 3)

    // Test 5: Drop partitions with MONTH interval
    sql "DROP TABLE IF EXISTS drop_range_partition_5 FORCE"
    sql """
        CREATE TABLE drop_range_partition_5
        (
            `k1` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `date`)
        PARTITION BY RANGE(`date`)
        (FROM ("2024-01-01") TO ("2024-07-01") INTERVAL 1 MONTH)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    List<List<Object>> result9 = sql "show partitions from drop_range_partition_5;"
    assertEquals(result9.size(), 6)
    
    sql "ALTER TABLE drop_range_partition_5 DROP PARTITION FROM ('2024-02-01') TO ('2024-05-01') INTERVAL 1 MONTH;"
    List<List<Object>> result10 = sql "show partitions from drop_range_partition_5;"
    assertEquals(result10.size(), 3)

    // Test 6: Drop partitions with different numeric intervals
    sql "DROP TABLE IF EXISTS drop_range_partition_6 FORCE"
    sql """
        CREATE TABLE drop_range_partition_6
        (
            `k1` LARGEINT NOT NULL,
            `score` INT,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `score`)
        PARTITION BY RANGE(`score`)
        (FROM (0) TO (1000) INTERVAL 100)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    List<List<Object>> result12 = sql "show partitions from drop_range_partition_6;"
    assertEquals(result12.size(), 10)
    
    sql "ALTER TABLE drop_range_partition_6 DROP PARTITION FROM (200) TO (800) INTERVAL 100;"
    List<List<Object>> result13 = sql "show partitions from drop_range_partition_6;"
    assertEquals(result13.size(), 4)

    // Test 8: Delete partitions by date number without intervals
    sql "DROP TABLE IF EXISTS drop_range_partition_7 FORCE"
    sql """
        CREATE TABLE drop_range_partition_7
        (
            `k1` LARGEINT NOT NULL,
            `age` SMALLINT,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `age`)
        PARTITION BY RANGE(`age`)
        (
            PARTITION p_1 VALUES [("1"), ("10")),
            PARTITION p_2 VALUES [("10"), ("20")),
            PARTITION p_3 VALUES [("20"), ("30")),
            PARTITION p_4 VALUES [("30"), ("40"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    
    sql "ALTER TABLE drop_range_partition_7 DROP PARTITION FROM (1) TO (3);"
    List<List<Object>> result14 = sql "show partitions from drop_range_partition_7;"
    assertEquals(result14.size(), 2)
    
    // Test 8: Delete partitions by date range without intervals
    sql "DROP TABLE IF EXISTS drop_range_partition_7 FORCE"
    sql """
        CREATE TABLE drop_range_partition_7
        (
            `k1` LARGEINT NOT NULL,
            `age` SMALLINT,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `age`)
        PARTITION BY RANGE(`age`)
        (
            PARTITION p_1 VALUES [("1"), ("10")),
            PARTITION p_2 VALUES [("10"), ("20")),
            PARTITION p_3 VALUES [("20"), ("30")),
            PARTITION p_4 VALUES [("30"), ("40"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    
    sql "ALTER TABLE drop_range_partition_7 DROP PARTITION FROM (1) TO (3);"
    List<List<Object>> result15 = sql "show partitions from drop_range_partition_7;"
    assertEquals(result15.size(), 2)
}
