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

suite("test_modify_distribution_buckets") {

    // ============================================================
    // 1. Basic: modify fixed bucket count on a RANGE partitioned table (HASH)
    // ============================================================
    sql "drop table if exists mdb_range_hash"
    sql """
        CREATE TABLE mdb_range_hash (
            k1 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        PARTITION BY RANGE(k1) (
            PARTITION p1 VALUES LESS THAN ('10'),
            PARTITION p2 VALUES LESS THAN ('20')
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ('replication_num' = '1')
    """

    // existing partitions should have 5 buckets
    def parts = sql_return_maparray "show partitions from mdb_range_hash"
    assertEquals(2, parts.size())
    for (def p : parts) {
        assertEquals(5, Integer.valueOf(p.Buckets))
    }

    // alter to 10 buckets
    sql "ALTER TABLE mdb_range_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 10"

    // existing partitions still have 5 buckets (only default changes)
    parts = sql_return_maparray "show partitions from mdb_range_hash"
    for (def p : parts) {
        assertEquals(5, Integer.valueOf(p.Buckets))
    }

    // add a new partition — should use the new default 10
    sql "ALTER TABLE mdb_range_hash ADD PARTITION p3 VALUES LESS THAN ('30')"
    parts = sql_return_maparray "show partitions from mdb_range_hash"
    for (def p : parts) {
        if (p.PartitionName == "p3") {
            assertEquals(10, Integer.valueOf(p.Buckets))
        } else {
            assertEquals(5, Integer.valueOf(p.Buckets))
        }
    }

    // show create table should display BUCKETS 10
    def createResult = sql "show create table mdb_range_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 10"))

    // ============================================================
    // 2. Switch from fixed to AUTO bucket (HASH)
    // ============================================================
    sql "ALTER TABLE mdb_range_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS AUTO"

    createResult = sql "show create table mdb_range_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // add a partition under AUTO mode
    sql "ALTER TABLE mdb_range_hash ADD PARTITION p4 VALUES LESS THAN ('40')"
    parts = sql_return_maparray "show partitions from mdb_range_hash"
    def p4 = parts.find { it.PartitionName == "p4" }
    assertNotNull(p4)
    // AUTO bucket should assign a valid positive number
    assertTrue(Integer.valueOf(p4.Buckets) > 0)

    // ============================================================
    // 3. Switch from AUTO back to fixed (HASH)
    // ============================================================
    sql "ALTER TABLE mdb_range_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 3"

    createResult = sql "show create table mdb_range_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 3"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    sql "ALTER TABLE mdb_range_hash ADD PARTITION p5 VALUES LESS THAN ('50')"
    parts = sql_return_maparray "show partitions from mdb_range_hash"
    def p5 = parts.find { it.PartitionName == "p5" }
    assertEquals(3, Integer.valueOf(p5.Buckets))

    // ============================================================
    // 4. LIST partitioned table (HASH)
    // ============================================================
    sql "drop table if exists mdb_list_hash"
    sql """
        CREATE TABLE mdb_list_hash (
            k1 VARCHAR(20),
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        PARTITION BY LIST(k1) (
            PARTITION pa VALUES IN ('a'),
            PARTITION pb VALUES IN ('b')
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 4
        PROPERTIES ('replication_num' = '1')
    """

    sql "ALTER TABLE mdb_list_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 8"
    sql "ALTER TABLE mdb_list_hash ADD PARTITION pc VALUES IN ('c')"

    parts = sql_return_maparray "show partitions from mdb_list_hash"
    def pc = parts.find { it.PartitionName == "pc" }
    assertEquals(8, Integer.valueOf(pc.Buckets))
    // old partitions unchanged
    def pa = parts.find { it.PartitionName == "pa" }
    assertEquals(4, Integer.valueOf(pa.Buckets))

    // switch list table to AUTO
    sql "ALTER TABLE mdb_list_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS AUTO"
    createResult = sql "show create table mdb_list_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // switch back to fixed
    sql "ALTER TABLE mdb_list_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 6"
    createResult = sql "show create table mdb_list_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 6"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // ============================================================
    // 5. RANDOM distribution table
    // ============================================================
    sql "drop table if exists mdb_random"
    sql """
        CREATE TABLE mdb_random (
            k1 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        PARTITION BY RANGE(k1) (
            PARTITION p1 VALUES LESS THAN ('100')
        )
        DISTRIBUTED BY RANDOM BUCKETS 6
        PROPERTIES ('replication_num' = '1')
    """

    sql "ALTER TABLE mdb_random MODIFY DISTRIBUTION DISTRIBUTED BY RANDOM BUCKETS 12"
    sql "ALTER TABLE mdb_random ADD PARTITION p2 VALUES LESS THAN ('200')"

    parts = sql_return_maparray "show partitions from mdb_random"
    def rp2 = parts.find { it.PartitionName == "p2" }
    assertEquals(12, Integer.valueOf(rp2.Buckets))

    // switch random distribution to AUTO
    sql "ALTER TABLE mdb_random MODIFY DISTRIBUTION DISTRIBUTED BY RANDOM BUCKETS AUTO"
    createResult = sql "show create table mdb_random"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // switch back to fixed
    sql "ALTER TABLE mdb_random MODIFY DISTRIBUTION DISTRIBUTED BY RANDOM BUCKETS 4"
    createResult = sql "show create table mdb_random"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 4"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // ============================================================
    // 6. Table originally created with AUTO buckets — switch to fixed and back
    // ============================================================
    sql "drop table if exists mdb_auto_origin"
    sql """
        CREATE TABLE mdb_auto_origin (
            k1 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        PARTITION BY RANGE(k1) (
            PARTITION p1 VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(k1) BUCKETS AUTO
        PROPERTIES ('replication_num' = '1')
    """

    createResult = sql "show create table mdb_auto_origin"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // switch from AUTO to fixed 5
    sql "ALTER TABLE mdb_auto_origin MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 5"
    createResult = sql "show create table mdb_auto_origin"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 5"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    sql "ALTER TABLE mdb_auto_origin ADD PARTITION p2 VALUES LESS THAN ('20')"
    parts = sql_return_maparray "show partitions from mdb_auto_origin"
    def aop2 = parts.find { it.PartitionName == "p2" }
    assertEquals(5, Integer.valueOf(aop2.Buckets))

    // switch back to AUTO
    sql "ALTER TABLE mdb_auto_origin MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS AUTO"
    createResult = sql "show create table mdb_auto_origin"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // ============================================================
    // 7. AUTO PARTITION + HASH distribution: modify buckets
    // ============================================================
    sql "drop table if exists mdb_auto_partition_hash"
    sql """
        CREATE TABLE mdb_auto_partition_hash (
            k1 DATETIMEV2 NOT NULL,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        AUTO PARTITION BY RANGE (date_trunc(k1, 'day')) ()
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ('replication_num' = '1')
    """

    // insert to trigger auto partition creation
    sql "INSERT INTO mdb_auto_partition_hash VALUES ('2024-01-01', 1)"
    sql "INSERT INTO mdb_auto_partition_hash VALUES ('2024-01-02', 2)"

    parts = sql_return_maparray "show partitions from mdb_auto_partition_hash"
    assertEquals(2, parts.size())
    for (def p : parts) {
        assertEquals(5, Integer.valueOf(p.Buckets))
    }

    // modify to 8 buckets
    sql "ALTER TABLE mdb_auto_partition_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 8"
    createResult = sql "show create table mdb_auto_partition_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 8"))

    // new auto-created partition should use 8 buckets
    sql "INSERT INTO mdb_auto_partition_hash VALUES ('2024-01-03', 3)"
    parts = sql_return_maparray "show partitions from mdb_auto_partition_hash"
    assertEquals(3, parts.size())
    // find the new partition (the one not for 01-01 or 01-02)
    def newParts = parts.findAll { Integer.valueOf(it.Buckets) == 8 }
    assertTrue(newParts.size() >= 1)

    // switch auto partition table to AUTO bucket
    sql "ALTER TABLE mdb_auto_partition_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS AUTO"
    createResult = sql "show create table mdb_auto_partition_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // insert another day to trigger auto partition with auto bucket
    sql "INSERT INTO mdb_auto_partition_hash VALUES ('2024-01-04', 4)"
    parts = sql_return_maparray "show partitions from mdb_auto_partition_hash"
    assertEquals(4, parts.size())

    // switch back to fixed
    sql "ALTER TABLE mdb_auto_partition_hash MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 3"
    createResult = sql "show create table mdb_auto_partition_hash"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 3"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    sql "INSERT INTO mdb_auto_partition_hash VALUES ('2024-01-05', 5)"
    parts = sql_return_maparray "show partitions from mdb_auto_partition_hash"
    assertEquals(5, parts.size())
    // find the newest partition (for 2024-01-05)
    def p0105 = parts.findAll { Integer.valueOf(it.Buckets) == 3 }
    assertTrue(p0105.size() >= 1)

    // ============================================================
    // 8. AUTO PARTITION + LIST distribution: modify buckets
    // ============================================================
    sql "drop table if exists mdb_auto_partition_list"
    sql """
        CREATE TABLE mdb_auto_partition_list (
            k1 VARCHAR(20) NOT NULL,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        AUTO PARTITION BY LIST (k1) ()
        DISTRIBUTED BY HASH(k1) BUCKETS 4
        PROPERTIES ('replication_num' = '1')
    """

    sql "INSERT INTO mdb_auto_partition_list VALUES ('aaa', 1)"
    sql "INSERT INTO mdb_auto_partition_list VALUES ('bbb', 2)"

    parts = sql_return_maparray "show partitions from mdb_auto_partition_list"
    assertEquals(2, parts.size())
    for (def p : parts) {
        assertEquals(4, Integer.valueOf(p.Buckets))
    }

    // modify to 7 buckets
    sql "ALTER TABLE mdb_auto_partition_list MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 7"
    createResult = sql "show create table mdb_auto_partition_list"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 7"))

    // new auto partition should use 7 buckets
    sql "INSERT INTO mdb_auto_partition_list VALUES ('ccc', 3)"
    parts = sql_return_maparray "show partitions from mdb_auto_partition_list"
    assertEquals(3, parts.size())
    def newListParts = parts.findAll { Integer.valueOf(it.Buckets) == 7 }
    assertTrue(newListParts.size() >= 1)

    // switch to AUTO bucket
    sql "ALTER TABLE mdb_auto_partition_list MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS AUTO"
    createResult = sql "show create table mdb_auto_partition_list"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    sql "INSERT INTO mdb_auto_partition_list VALUES ('ddd', 4)"
    parts = sql_return_maparray "show partitions from mdb_auto_partition_list"
    assertEquals(4, parts.size())

    // switch back to fixed
    sql "ALTER TABLE mdb_auto_partition_list MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 2"
    createResult = sql "show create table mdb_auto_partition_list"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 2"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // ============================================================
    // 9. Error cases
    // ============================================================

    // 9a. Colocate table should be rejected
    sql "drop table if exists mdb_colocate"
    sql """
        CREATE TABLE mdb_colocate (
            k1 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        PARTITION BY RANGE(k1) (
            PARTITION p1 VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES (
            'replication_num' = '1',
            'colocate_with' = 'mdb_colocate_group'
        )
    """

    test {
        sql "ALTER TABLE mdb_colocate MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 10"
        exception "Cannot change default bucket number of colocate table"
    }

    // 9b. Unpartitioned table should be rejected
    sql "drop table if exists mdb_no_partition"
    sql """
        CREATE TABLE mdb_no_partition (
            k1 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ('replication_num' = '1')
    """

    test {
        sql "ALTER TABLE mdb_no_partition MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 10"
        exception "Only support change partitioned table's distribution"
    }

    // 9c. Cannot change distribution type (HASH -> RANDOM)
    test {
        sql "ALTER TABLE mdb_range_hash MODIFY DISTRIBUTION DISTRIBUTED BY RANDOM BUCKETS 10"
        exception "Cannot change distribution type"
    }

    // 9d. Cannot change distribution columns
    sql "drop table if exists mdb_hash_cols"
    sql """
        CREATE TABLE mdb_hash_cols (
            k1 INT,
            k2 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2, v1)
        PARTITION BY RANGE(k1) (
            PARTITION p1 VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 5
        PROPERTIES ('replication_num' = '1')
    """

    test {
        sql "ALTER TABLE mdb_hash_cols MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k2) BUCKETS 10"
        exception "Cannot assign hash distribution with different distribution cols"
    }

    // ============================================================
    // 10. Multiple sequential modifications
    // ============================================================
    sql "drop table if exists mdb_multi_alter"
    sql """
        CREATE TABLE mdb_multi_alter (
            k1 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        PARTITION BY RANGE(k1) (
            PARTITION p1 VALUES LESS THAN ('10')
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ('replication_num' = '1')
    """

    sql "ALTER TABLE mdb_multi_alter MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 4"
    sql "ALTER TABLE mdb_multi_alter ADD PARTITION p2 VALUES LESS THAN ('20')"

    sql "ALTER TABLE mdb_multi_alter MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS AUTO"
    sql "ALTER TABLE mdb_multi_alter ADD PARTITION p3 VALUES LESS THAN ('30')"

    sql "ALTER TABLE mdb_multi_alter MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 6"
    sql "ALTER TABLE mdb_multi_alter ADD PARTITION p4 VALUES LESS THAN ('40')"

    parts = sql_return_maparray "show partitions from mdb_multi_alter"
    assertEquals(4, parts.size())
    assertEquals(2, Integer.valueOf(parts.find { it.PartitionName == "p1" }.Buckets))
    assertEquals(4, Integer.valueOf(parts.find { it.PartitionName == "p2" }.Buckets))
    // p3 was added under AUTO mode - just ensure valid bucket count
    assertTrue(Integer.valueOf(parts.find { it.PartitionName == "p3" }.Buckets) > 0)
    assertEquals(6, Integer.valueOf(parts.find { it.PartitionName == "p4" }.Buckets))

    createResult = sql "show create table mdb_multi_alter"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 6"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // ============================================================
    // 11. AUTO PARTITION table created with AUTO bucket — full lifecycle
    // ============================================================
    sql "drop table if exists mdb_auto_auto"
    sql """
        CREATE TABLE mdb_auto_auto (
            k1 DATETIMEV2 NOT NULL,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v1)
        AUTO PARTITION BY RANGE (date_trunc(k1, 'month')) ()
        DISTRIBUTED BY HASH(k1) BUCKETS AUTO
        PROPERTIES ('replication_num' = '1')
    """

    createResult = sql "show create table mdb_auto_auto"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    // insert to create partitions under AUTO bucket
    sql "INSERT INTO mdb_auto_auto VALUES ('2024-01-15', 1)"
    sql "INSERT INTO mdb_auto_auto VALUES ('2024-02-15', 2)"
    parts = sql_return_maparray "show partitions from mdb_auto_auto"
    assertEquals(2, parts.size())

    // switch to fixed 5
    sql "ALTER TABLE mdb_auto_auto MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 5"
    createResult = sql "show create table mdb_auto_auto"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS 5"))
    assertFalse(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    sql "INSERT INTO mdb_auto_auto VALUES ('2024-03-15', 3)"
    parts = sql_return_maparray "show partitions from mdb_auto_auto"
    assertEquals(3, parts.size())
    def marchPart = parts.findAll { Integer.valueOf(it.Buckets) == 5 }
    assertTrue(marchPart.size() >= 1)

    // switch back to AUTO
    sql "ALTER TABLE mdb_auto_auto MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS AUTO"
    createResult = sql "show create table mdb_auto_auto"
    assertTrue(createResult.toString().containsIgnoreCase("BUCKETS AUTO"))

    sql "INSERT INTO mdb_auto_auto VALUES ('2024-04-15', 4)"
    parts = sql_return_maparray "show partitions from mdb_auto_auto"
    assertEquals(4, parts.size())
}
