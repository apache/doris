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

suite("test_replace_partition_with_version", "p0") {
    def tbName = "test_replace_partition_version_tbl"
    def dropSql = "DROP TABLE IF EXISTS ${tbName}"
    
    def initTable = "CREATE TABLE IF NOT EXISTS ${tbName}\n" +
            "(\n" +
            "    `id` INT NOT NULL,\n" +
            "    `name` VARCHAR(50),\n" +
            "    `date_col` DATE NOT NULL\n" +
            ")\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`id`)\n" +
            "PARTITION BY RANGE(`date_col`)\n" +
            "(\n" +
            "    PARTITION `p20240601` VALUES [(\"2024-06-01\"), (\"2024-06-02\")),\n" +
            "    PARTITION `p20240602` VALUES [(\"2024-06-02\"), (\"2024-06-03\"))\n" +
            ")\n" +
            "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
            "PROPERTIES\n" +
            "(\n" +
            "    \"replication_num\" = \"1\"\n" +
            ");"
    
    // Test 1: Basic replace partition without version checking (legacy behavior)
    sql dropSql
    sql initTable
    
    // Insert test data
    sql "INSERT INTO ${tbName} VALUES (1, 'Alice', '2024-06-01'), (2, 'Bob', '2024-06-02')"
    
    // Create temporary partition
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240601 VALUES [(\"2024-06-01\"), (\"2024-06-02\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    
    // Insert data into temporary partition
    sql "INSERT INTO ${tbName} TEMPORARY PARTITION(tp20240601) VALUES (1, 'Alice Updated', '2024-06-01')"
    
    // Replace partition without version checking (should work)
    sql "ALTER TABLE ${tbName} REPLACE PARTITION (p20240601) WITH TEMPORARY PARTITION(tp20240601)"
    
    // Verify data
    def result = sql "SELECT * FROM ${tbName} WHERE date_col = '2024-06-01'"
    assertEquals(1, result.size())
    assertEquals('Alice Updated', result[0][1])
    
    // Test 2: Replace partition with version checking - success case
    sql dropSql
    sql initTable
    
    // Insert test data
    sql "INSERT INTO ${tbName} VALUES (1, 'Alice', '2024-06-01'), (2, 'Bob', '2024-06-02')"
    
    // Get current partition version
    def partitionInfo = sql_return_maparray "SHOW PARTITIONS FROM ${tbName}"
    def p20240602Version = null
    for (def partition : partitionInfo) {
        if (partition.PartitionName == 'p20240602') {
            p20240602Version = partition.VisibleVersion as long
            break
        }
    }
    
    assertNotNull(p20240602Version, "Partition version should not be null")
    
    // Create temporary partition
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240602 VALUES [(\"2024-06-02\"), (\"2024-06-03\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    
    // Insert data into temporary partition
    sql "INSERT INTO ${tbName} TEMPORARY PARTITION(tp20240602) VALUES (2, 'Bob Updated', '2024-06-02')"
    
    // Replace partition with correct version (should work)
    sql "ALTER TABLE ${tbName} REPLACE PARTITION (p20240602) WITH TEMPORARY PARTITION(tp20240602) PROPERTIES(\"expected_versions\" = \"p20240602:${p20240602Version}\")"
    
    // Verify data
    result = sql "SELECT * FROM ${tbName} WHERE date_col = '2024-06-02'"
    assertEquals(1, result.size())
    assertEquals('Bob Updated', result[0][1])
    
    // Test 3: Replace partition with version checking - failure case (version mismatch)
    sql dropSql
    sql initTable
    
    // Insert test data
    sql "INSERT INTO ${tbName} VALUES (1, 'Alice', '2024-06-01'), (2, 'Bob', '2024-06-02')"
    
    // Create temporary partition
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240601_fail VALUES [(\"2024-06-01\"), (\"2024-06-02\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    
    // Insert data into temporary partition
    sql "INSERT INTO ${tbName} TEMPORARY PARTITION(tp20240601_fail) VALUES (1, 'Alice Failed', '2024-06-01')"
    
    // Try to replace partition with wrong version (should fail)
    def errorMessage = "version mismatch"
    // 将第111行的 expectException 改为 expectExceptionLike
    expectExceptionLike({
    sql "ALTER TABLE ${tbName} REPLACE PARTITION (p20240601) WITH TEMPORARY PARTITION(tp20240601_fail) PROPERTIES(\"expected_versions\" = \"p20240601:999\")"
    }, "version mismatch")
    
    // Verify original data is unchanged
    result = sql "SELECT * FROM ${tbName} WHERE date_col = '2024-06-01'"
    assertEquals(1, result.size())
    assertEquals('Alice', result[0][1])
    
    // Test 4: Multiple partitions version checking
    sql dropSql
    sql initTable
    
    // Insert test data
    sql "INSERT INTO ${tbName} VALUES (1, 'Alice', '2024-06-01'), (2, 'Bob', '2024-06-02')"
    
    // Get current partition versions
    partitionInfo = sql_return_maparray "SHOW PARTITIONS FROM ${tbName}"
    def p20240601Version = null
    p20240602Version = null
    for (def partition : partitionInfo) {
        if (partition.PartitionName == 'p20240601') {
            p20240601Version = partition.VisibleVersion as long
        } else if (partition.PartitionName == 'p20240602') {
            p20240602Version = partition.VisibleVersion as long
        }
    }
    
    assertNotNull(p20240601Version, "Partition p20240601 version should not be null")
    assertNotNull(p20240602Version, "Partition p20240602 version should not be null")
    
    // Create temporary partitions
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240601_multi VALUES [(\"2024-06-01\"), (\"2024-06-02\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240602_multi VALUES [(\"2024-06-02\"), (\"2024-06-03\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    
    // Insert data into temporary partitions
    sql "INSERT INTO ${tbName} TEMPORARY PARTITION(tp20240601_multi) VALUES (1, 'Alice Multi', '2024-06-01')"
    sql "INSERT INTO ${tbName} TEMPORARY PARTITION(tp20240602_multi) VALUES (2, 'Bob Multi', '2024-06-02')"
    
    // Replace multiple partitions with version checking (should work)
    sql "ALTER TABLE ${tbName} REPLACE PARTITION (p20240601, p20240602) WITH TEMPORARY PARTITION(tp20240601_multi, tp20240602_multi) PROPERTIES(\"expected_versions\" = \"p20240601:${p20240601Version}, p20240602:${p20240602Version}\")"
    
    // Verify data
    result = sql "SELECT * FROM ${tbName} ORDER BY date_col"
    assertEquals(2, result.size())
    assertEquals('Alice Multi', result[0][1])
    assertEquals('Bob Multi', result[1][1])
    
    // Test 5: Invalid version format
    sql dropSql
    sql initTable
    
    // Insert test data
    sql "INSERT INTO ${tbName} VALUES (1, 'Alice', '2024-06-01')"
    
    // Create temporary partition
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240601_invalid VALUES [(\"2024-06-01\"), (\"2024-06-02\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    
    // Try to replace partition with invalid version format (should fail)
    errorMessage = "Invalid expected_versions format"
    expectExceptionLike({
    sql "ALTER TABLE ${tbName} REPLACE PARTITION (p20240601) WITH TEMPORARY PARTITION(tp20240601_invalid) PROPERTIES(\"expected_versions\" = \"invalid_format\")"
    }, errorMessage)
    
    // Test 6: Mismatch between expected_versions and replace partitions count
    sql dropSql
    sql initTable
    
    // Insert test data
    sql "INSERT INTO ${tbName} VALUES (1, 'Alice', '2024-06-01'), (2, 'Bob', '2024-06-02')"
    
    // Get current partition version for p20240601 only
    partitionInfo = sql_return_maparray "SHOW PARTITIONS FROM ${tbName}"
    p20240601Version = null
    for (def partition : partitionInfo) {
        if (partition.PartitionName == 'p20240601') {
            p20240601Version = partition.VisibleVersion as long
            break
        }
    }
    
    assertNotNull(p20240601Version, "Partition p20240601 version should not be null")
    
    // Create temporary partitions
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240601_mismatch VALUES [(\"2024-06-01\"), (\"2024-06-02\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    sql "ALTER TABLE ${tbName} ADD TEMPORARY PARTITION tp20240602_mismatch VALUES [(\"2024-06-02\"), (\"2024-06-03\")) DISTRIBUTED BY HASH(`id`) BUCKETS 4"
    
    // Insert data into temporary partitions
    sql "INSERT INTO ${tbName} TEMPORARY PARTITION(tp20240601_mismatch) VALUES (1, 'Alice Mismatch', '2024-06-01')"
    sql "INSERT INTO ${tbName} TEMPORARY PARTITION(tp20240602_mismatch) VALUES (2, 'Bob Mismatch', '2024-06-02')"
    
    // Try to replace two partitions but only provide version for one (should fail)
    errorMessage = "Partitions in expected_versions must exactly match the partitions to be replaced"
    expectExceptionLike({
    sql "ALTER TABLE ${tbName} REPLACE PARTITION (p20240601, p20240602) WITH TEMPORARY PARTITION(tp20240601_mismatch, tp20240602_mismatch) PROPERTIES(\"expected_versions\" = \"p20240601:${p20240601Version}\")"
    }, errorMessage)
    
    // Verify original data is unchanged
    result = sql "SELECT * FROM ${tbName} ORDER BY date_col"
    assertEquals(2, result.size())
    assertEquals('Alice', result[0][1])
    assertEquals('Bob', result[1][1])
    
    // Clean up
    sql dropSql
}