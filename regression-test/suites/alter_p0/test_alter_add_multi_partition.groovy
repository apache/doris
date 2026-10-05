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

    // Check if you can create a partition for a leap month
    sql """
        CREATE TABLE IF NOT EXISTS add_multi_partition
        (
            `k1` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `k2` VARCHAR(20)
        )
        ENGINE=OLAP
        UNIQUE KEY(`k1`, `date`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION `p_20000201` VALUES [("2000-02-01"), ("2000-02-05"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    List<List<Object>> result1  = sql "show partitions from add_multi_partition;"
    assertEquals(result1.size(), 1)
    sql "ALTER TABLE add_multi_partition ADD PARTITIONS FROM ('2000-02-05') TO ('2000-03-01') INTERVAL 4 DAY;"
    List<List<Object>> result2  = sql "show partitions from add_multi_partition;"
    assertEquals(result2.size(), 8)
    def partitionName = sql "show partitions from add_multi_partition where PartitionName = 'p_20000229';"
    for (pn in partitionName) {
        assertTrue(pn[1] == "p_20000229")
    }
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"

    
    sql """
        CREATE TABLE `add_multi_partition` (
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
    List<List<Object>> result3  = sql "show partitions from add_multi_partition;"
    assertEquals(result3.size(), 9)
    sql "ALTER TABLE add_multi_partition ADD PARTITIONS FROM ('2024-04-01') TO ('2025-01-01') INTERVAL 1 WEEK;"
    List<List<Object>> result4  = sql "show partitions from add_multi_partition;"
    assertEquals(result4.size(), 49)
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"

    
    sql """
        CREATE TABLE IF NOT EXISTS add_multi_partition
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
    List<List<Object>> result7  = sql "show partitions from add_multi_partition;"
    assertEquals(result7.size(), 10)
    sql "ALTER TABLE add_multi_partition ADD PARTITIONS FROM (100) TO (200) INTERVAL 10;"
    List<List<Object>> result8  = sql "show partitions from add_multi_partition;"
    assertEquals(result8.size(), 20)
    sql "DROP TABLE IF EXISTS add_multi_partition FORCE"

}