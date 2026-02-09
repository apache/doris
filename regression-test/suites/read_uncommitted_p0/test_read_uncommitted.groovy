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

suite("test_read_uncommitted") {
    // ---- Test 1: Session variable setting and reading ----
    // Verify that transaction_isolation can be set to READ-UNCOMMITTED
    // and read back correctly.
    sql "SET transaction_isolation = 'READ-UNCOMMITTED'"
    def result = sql "SHOW VARIABLES LIKE 'transaction_isolation'"
    assertTrue(result.size() > 0)
    assertTrue(result[0][1].toString().equalsIgnoreCase("READ-UNCOMMITTED"))

    // Also verify tx_isolation alias returns the same value
    def result2 = sql "SHOW VARIABLES LIKE 'tx_isolation'"
    assertTrue(result2.size() > 0)
    assertTrue(result2[0][1].toString().equalsIgnoreCase("READ-UNCOMMITTED"))

    // Reset to default
    sql "SET transaction_isolation = 'READ-COMMITTED'"

    // ---- Test 2: Query duplicate key table under READ-UNCOMMITTED ----
    def dupTable = "test_read_uncommitted_dup"
    sql "DROP TABLE IF EXISTS ${dupTable}"
    sql """
        CREATE TABLE ${dupTable} (
            k1 INT NOT NULL,
            v1 VARCHAR(64) NOT NULL,
            v2 INT NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO ${dupTable} VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)"
    sql "INSERT INTO ${dupTable} VALUES (4, 'd', 40), (5, 'e', 50)"

    // Query under READ-UNCOMMITTED should see all committed data
    sql "SET transaction_isolation = 'READ-UNCOMMITTED'"
    def dupResult = sql "SELECT * FROM ${dupTable} ORDER BY k1"
    assertEquals(5, dupResult.size())
    assertEquals(1, dupResult[0][0])
    assertEquals(5, dupResult[4][0])

    // Aggregation should work
    def aggResult = sql "SELECT COUNT(*), SUM(v2) FROM ${dupTable}"
    assertEquals(1, aggResult.size())
    assertEquals(5, aggResult[0][0] as int)
    assertEquals(150, aggResult[0][1] as int)

    sql "SET transaction_isolation = 'READ-COMMITTED'"
    sql "DROP TABLE IF EXISTS ${dupTable}"

    // ---- Test 3: Query unique key MoW table under READ-UNCOMMITTED ----
    def mowTable = "test_read_uncommitted_mow"
    sql "DROP TABLE IF EXISTS ${mowTable}"
    sql """
        CREATE TABLE ${mowTable} (
            k1 INT NOT NULL,
            v1 VARCHAR(64) NOT NULL,
            v2 INT NOT NULL
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql "INSERT INTO ${mowTable} VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)"
    // Update existing key
    sql "INSERT INTO ${mowTable} VALUES (2, 'b_updated', 25)"

    sql "SET transaction_isolation = 'READ-UNCOMMITTED'"
    def mowResult = sql "SELECT * FROM ${mowTable} ORDER BY k1"
    assertEquals(3, mowResult.size())
    assertEquals("b_updated", mowResult[1][1])
    assertEquals(25, mowResult[1][2])

    sql "SET transaction_isolation = 'READ-COMMITTED'"
    sql "DROP TABLE IF EXISTS ${mowTable}"

    // ---- Test 4: Query unique key MoR table under READ-UNCOMMITTED ----
    def morTable = "test_read_uncommitted_mor"
    sql "DROP TABLE IF EXISTS ${morTable}"
    sql """
        CREATE TABLE ${morTable} (
            k1 INT NOT NULL,
            v1 VARCHAR(64) NOT NULL,
            v2 INT NOT NULL
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        )
    """

    sql "INSERT INTO ${morTable} VALUES (1, 'x', 100), (2, 'y', 200)"
    sql "INSERT INTO ${morTable} VALUES (2, 'y_updated', 250), (3, 'z', 300)"

    sql "SET transaction_isolation = 'READ-UNCOMMITTED'"
    def morResult = sql "SELECT * FROM ${morTable} ORDER BY k1"
    assertEquals(3, morResult.size())
    assertEquals("y_updated", morResult[1][1])
    assertEquals(250, morResult[1][2])

    sql "SET transaction_isolation = 'READ-COMMITTED'"
    sql "DROP TABLE IF EXISTS ${morTable}"

    // ---- Test 5: Switching between isolation levels mid-session ----
    def switchTable = "test_read_uncommitted_switch"
    sql "DROP TABLE IF EXISTS ${switchTable}"
    sql """
        CREATE TABLE ${switchTable} (
            k1 INT NOT NULL,
            v1 INT NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO ${switchTable} VALUES (1, 10), (2, 20)"

    // Query under READ-COMMITTED
    sql "SET transaction_isolation = 'READ-COMMITTED'"
    def rcResult = sql "SELECT COUNT(*) FROM ${switchTable}"
    assertEquals(2, rcResult[0][0] as int)

    // Switch to READ-UNCOMMITTED and query again
    sql "SET transaction_isolation = 'READ-UNCOMMITTED'"
    def ruResult = sql "SELECT COUNT(*) FROM ${switchTable}"
    assertEquals(2, ruResult[0][0] as int)

    // Switch back to READ-COMMITTED
    sql "SET transaction_isolation = 'READ-COMMITTED'"
    def rc2Result = sql "SELECT COUNT(*) FROM ${switchTable}"
    assertEquals(2, rc2Result[0][0] as int)

    sql "DROP TABLE IF EXISTS ${switchTable}"

    // ---- Test 6: Invalid isolation level rejected ----
    test {
        sql "SET transaction_isolation = 'INVALID-LEVEL'"
        exception "Unsupported transaction isolation level"
    }

    // ---- Test 7: Predicates and filters work under READ-UNCOMMITTED ----
    def filterTable = "test_read_uncommitted_filter"
    sql "DROP TABLE IF EXISTS ${filterTable}"
    sql """
        CREATE TABLE ${filterTable} (
            k1 INT NOT NULL,
            v1 VARCHAR(64) NOT NULL,
            v2 INT NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO ${filterTable} VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50)"

    sql "SET transaction_isolation = 'READ-UNCOMMITTED'"

    // WHERE filter
    def filterResult = sql "SELECT * FROM ${filterTable} WHERE v2 > 25 ORDER BY k1"
    assertEquals(3, filterResult.size())
    assertEquals(3, filterResult[0][0])

    // LIMIT
    def limitResult = sql "SELECT * FROM ${filterTable} ORDER BY k1 LIMIT 2"
    assertEquals(2, limitResult.size())

    // GROUP BY
    def groupResult = sql "SELECT v1, SUM(v2) FROM ${filterTable} GROUP BY v1 ORDER BY v1"
    assertEquals(5, groupResult.size())

    sql "SET transaction_isolation = 'READ-COMMITTED'"
    sql "DROP TABLE IF EXISTS ${filterTable}"
}
