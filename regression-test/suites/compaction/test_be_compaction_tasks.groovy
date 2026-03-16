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

suite("test_be_compaction_tasks") {
    def tableName = "test_be_compaction_tasks"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT NOT NULL,
            name STRING NOT NULL
        ) DUPLICATE KEY (`id`)
          DISTRIBUTED BY HASH(`id`) BUCKETS 1
          PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    // Insert multiple batches to create rowsets for compaction
    for (i in 0..<10) {
        sql """ INSERT INTO ${tableName} VALUES(${i}, "row_${i}") """
    }

    // 1. Test: basic query returns valid result set
    def result1 = sql """ SELECT * FROM information_schema.be_compaction_tasks LIMIT 10 """
    log.info("Initial query result size: ${result1.size()}")

    // 2. Trigger a cumulative compaction via HTTP API
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    def tablets = sql_return_maparray """ show tablets from ${tableName} """
    def tablet = tablets[0]
    def tablet_id = tablet.TabletId
    def be_host = backendId_to_backendIP["${tablet.BackendId}"]
    def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]

    def (code2, text2, err2) = be_run_cumulative_compaction(be_host, be_port, tablet_id)
    log.info("Trigger compaction response: ${text2}")

    // Wait for compaction to finish
    def running = true
    def maxWait = 30
    while (running && maxWait > 0) {
        Thread.sleep(1000)
        def (code, out, err) = be_get_compaction_status(be_host, be_port, tablet_id)
        def status = parseJson(out.trim())
        running = status.run_status
        maxWait--
    }
    assertFalse(running, "Compaction did not finish in time")

    // 3. Test: FINISHED record should appear with correct fields
    def result3 = sql """
        SELECT COMPACTION_ID, COMPACTION_TYPE, STATUS, TRIGGER_METHOD,
               TABLET_ID, INPUT_ROWSETS_COUNT, INPUT_ROW_NUM
        FROM information_schema.be_compaction_tasks
        WHERE TABLET_ID = ${tablet_id} AND STATUS = 'FINISHED'
        ORDER BY COMPACTION_ID DESC
        LIMIT 1
    """
    log.info("Compaction tasks for tablet: ${result3}")
    assertTrue(result3.size() > 0, "Expected at least one FINISHED record after compaction")

    def row = result3[0]
    assertNotNull(row[0], "COMPACTION_ID should not be null")
    assertEquals("cumulative", row[1].toString())
    assertEquals("FINISHED", row[2].toString())
    assertEquals("MANUAL", row[3].toString())
    assertTrue(Long.parseLong(row[5].toString()) > 0, "INPUT_ROWSETS_COUNT should be > 0")
    assertTrue(Long.parseLong(row[6].toString()) > 0, "INPUT_ROW_NUM should be > 0")

    // 4. Test: filter by STATUS
    def result4 = sql """
        SELECT COUNT(*) FROM information_schema.be_compaction_tasks
        WHERE STATUS = 'FINISHED'
    """
    assertTrue(Long.parseLong(result4[0][0].toString()) >= 1)

    // 5. Test: BACKEND_ID is populated
    def result5 = sql """
        SELECT BACKEND_ID FROM information_schema.be_compaction_tasks
        WHERE TABLET_ID = ${tablet_id}
        LIMIT 1
    """
    assertTrue(result5.size() > 0)
    assertTrue(Long.parseLong(result5[0][0].toString()) > 0, "BACKEND_ID should be positive")

    // 6. Test: non-existent tablet returns empty
    def result6 = sql """
        SELECT * FROM information_schema.be_compaction_tasks
        WHERE TABLET_ID = 999999999
    """
    assertEquals(0, result6.size())

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
