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

suite("test_show_catalog_recycle_bin_where") {
    sql "DROP DATABASE IF EXISTS `test_show_catalog_recycle_bin_where_db`"
    sql "CREATE DATABASE `test_show_catalog_recycle_bin_where_db`"

    sql """
        CREATE TABLE `test_show_catalog_recycle_bin_where_db`.`test_show_catalog_recycle_bin_where_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        PARTITION BY RANGE(`k1`)
        (
            PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333'))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    def dbId = getDbId("test_show_catalog_recycle_bin_where_db") + ''
    def tableId = getTableId("test_show_catalog_recycle_bin_where_db",
            "test_show_catalog_recycle_bin_where_tb") + ''
    logger.info("dbId: " + dbId)
    logger.info("tableId: " + tableId)

    // drop a partition
    sql "ALTER TABLE `test_show_catalog_recycle_bin_where_db`.`test_show_catalog_recycle_bin_where_tb` DROP PARTITION p111"

    // drop the table
    sql "DROP TABLE `test_show_catalog_recycle_bin_where_db`.`test_show_catalog_recycle_bin_where_tb`"

    // drop the database
    sql "DROP DATABASE `test_show_catalog_recycle_bin_where_db`"

    // =====================================================================
    // 1. Single-condition precise filtering validation
    // =====================================================================

    // test Type filter: only show Partition entries
    def partitionRes = sql """ SHOW CATALOG RECYCLE BIN WHERE Type = "Partition" """
    assertTrue(partitionRes.size() > 0)
    for (def row : partitionRes) {
        assertEquals("Partition", row[0])
    }

    // test Type filter: only show Database entries
    def dbRes = sql """ SHOW CATALOG RECYCLE BIN WHERE Type = "Database" """
    assertTrue(dbRes.size() > 0)
    for (def row : dbRes) {
        assertEquals("Database", row[0])
    }

    // test Type filter: only show Table entries
    def tableRes = sql """ SHOW CATALOG RECYCLE BIN WHERE Type = "Table" """
    assertTrue(tableRes.size() > 0)
    for (def row : tableRes) {
        assertEquals("Table", row[0])
    }

    // test DbId filter
    def dbIdRes = sql """ SHOW CATALOG RECYCLE BIN WHERE DbId = "${dbId}" """
    assertTrue(dbIdRes.size() > 0)
    for (def row : dbIdRes) {
        assertEquals(dbId, row[2])
    }

    // test TableId filter: show table and its partitions
    def tableIdRes = sql """ SHOW CATALOG RECYCLE BIN WHERE TableId = "${tableId}" """
    assertTrue(tableIdRes.size() > 0)
    for (def row : tableIdRes) {
        assertEquals(tableId, row[3])
    }

    // =====================================================================
    // 2. Composite condition (AND) precise filtering validation
    // =====================================================================

    // test TableId + Type AND filter: show only return Partition (p111) drop in table
    def tableIdAndTypeRes = sql """ SHOW CATALOG RECYCLE BIN WHERE TableId = "${tableId}" AND Type = "Partition" """
    assertTrue(tableIdAndTypeRes.size() > 0)
    for (def row : tableIdAndTypeRes) {
        assertEquals(tableId, row[3])
        assertEquals("Partition", row[0])
    }

    // test Name + Type AND filter: only p111 partition should remain
    def nameAndTypeRes = sql """ SHOW CATALOG RECYCLE BIN WHERE Name = "p111" AND Type = "Partition" """
    assertTrue(nameAndTypeRes.size() > 0)
    for (def row : nameAndTypeRes) {
        assertEquals("p111", row[1])
        assertEquals("Partition", row[0])
    }

    // test Name + DbId + Type + TableId = 4 AND filter
    def quadAndRes = sql """ SHOW CATALOG RECYCLE BIN WHERE Name = "p111" AND Type = "Partition" AND DbId = "${dbId}" AND TableId = "${tableId}" """
    assertTrue(quadAndRes.size() == 1)
    def targetRow = quadAndRes.get(0)
    assertEquals("Partition", targetRow[0])
    assertEquals("p111", targetRow[1])
    assertEquals(dbId, targetRow[2])
    assertEquals(tableId, targetRow[3])

    // test Name LIKE + Type + DbId AND filter: Fuzzy matching combination
    def likeAndTypeRes = sql """ SHOW CATALOG RECYCLE BIN WHERE Name LIKE "p%" AND Type = "Partition" AND DbId = "${dbId}" """
    assertTrue(likeAndTypeRes.size() > 0)
    for (def row : likeAndTypeRes) {
        assertTrue(row[1].startsWith("p"))
        assertEquals("Partition", row[0])
        assertEquals(dbId, row[2])
    }

    // =====================================================================
    // 3. Abnormal conditions and conflict interception verification
    // =====================================================================

    // test unsupported column should fail
    test {
        sql """ SHOW CATALOG RECYCLE BIN WHERE DataSize = "100" """
        exception "Where clause should like"
    }

    // test unsupported operator should fail
    test {
        sql """ SHOW CATALOG RECYCLE BIN WHERE Name > "p111" """
        exception "Where clause should like"
    }

    // test duplicate filters on the same column should fail
    test {
        sql """ SHOW CATALOG RECYCLE BIN WHERE Type = "Partition" AND Type = "Table" """
        exception "Where clause should like"
    }

    test {
        sql """ SHOW CATALOG RECYCLE BIN WHERE TableId = "${tableId}" AND TableId = "99999" """
        exception "Where clause should like"
    }

    test {
        sql """ SHOW CATALOG RECYCLE BIN WHERE Name = "p111" AND Name LIKE "p%" """
        exception "Where clause should like"
    }

    // test same value duplicate LIKE should succeed
    test {
        sql """ SHOW CATALOG RECYCLE BIN WHERE Name LIKE "p%" AND Name LIKE "p%" """
    }
}
