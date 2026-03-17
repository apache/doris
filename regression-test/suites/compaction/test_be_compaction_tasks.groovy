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

    // Trigger cumulative compaction with retry
    def triggerSuccess = false
    for (int attempt = 0; attempt < 3; attempt++) {
        def (code2, text2, err2) = be_run_cumulative_compaction(be_host, be_port, tablet_id)
        log.info("Trigger compaction attempt ${attempt}: code=${code2}, out=${text2}, err=${err2}")
        if (text2 != null && !text2.trim().isEmpty()) {
            def triggerResult = parseJson(text2.trim())
            if (triggerResult.status.toLowerCase() == "success") {
                triggerSuccess = true
                break
            }
        }
        Thread.sleep(2000)
    }
    assertTrue(triggerSuccess, "Failed to trigger cumulative compaction")

    // Wait for compaction to finish
    def running = true
    def maxWait = 30
    while (running && maxWait > 0) {
        Thread.sleep(1000)
        def (code, out, err) = be_get_compaction_status(be_host, be_port, tablet_id)
        log.info("Compaction status: code=${code}, out=${out}, err=${err}")
        if (out == null || out.trim().isEmpty()) {
            maxWait--
            continue
        }
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

    // 7. Test: SELECT * to verify all 30 columns are queryable and log full row
    def resultAll = sql """
        SELECT * FROM information_schema.be_compaction_tasks
        WHERE TABLET_ID = ${tablet_id}
        ORDER BY COMPACTION_ID DESC
        LIMIT 1
    """
    assertTrue(resultAll.size() > 0, "Expected at least one record for SELECT *")
    assertEquals(30, resultAll[0].size(), "Expected 30 columns in be_compaction_tasks")
    log.info("Full row (SELECT *): ${resultAll[0]}")

    // 8. Test: query all 30 columns by name to verify each column is accessible
    def resultNamed = sql """
        SELECT BACKEND_ID, COMPACTION_ID, TABLE_ID, PARTITION_ID, TABLET_ID,
               COMPACTION_TYPE, STATUS, TRIGGER_METHOD, COMPACTION_SCORE,
               SCHEDULED_TIME, START_TIME, END_TIME, ELAPSED_TIME_MS,
               INPUT_ROWSETS_COUNT, INPUT_ROW_NUM, INPUT_DATA_SIZE, INPUT_SEGMENTS_NUM, INPUT_VERSION_RANGE,
               MERGED_ROWS, FILTERED_ROWS,
               OUTPUT_ROW_NUM, OUTPUT_DATA_SIZE, OUTPUT_SEGMENTS_NUM, OUTPUT_VERSION,
               BYTES_READ_FROM_LOCAL, BYTES_READ_FROM_REMOTE, PEAK_MEMORY_BYTES,
               IS_VERTICAL, PERMITS, STATUS_MSG
        FROM information_schema.be_compaction_tasks
        WHERE TABLET_ID = ${tablet_id} AND STATUS = 'FINISHED'
        ORDER BY COMPACTION_ID DESC
        LIMIT 1
    """
    assertTrue(resultNamed.size() > 0)
    def r = resultNamed[0]
    // Log each column value for visual inspection
    log.info("=== be_compaction_tasks full column dump ===")
    log.info("BACKEND_ID:            ${r[0]}")
    log.info("COMPACTION_ID:         ${r[1]}")
    log.info("TABLE_ID:              ${r[2]}")
    log.info("PARTITION_ID:          ${r[3]}")
    log.info("TABLET_ID:             ${r[4]}")
    log.info("COMPACTION_TYPE:       ${r[5]}")
    log.info("STATUS:                ${r[6]}")
    log.info("TRIGGER_METHOD:        ${r[7]}")
    log.info("COMPACTION_SCORE:      ${r[8]}")
    log.info("SCHEDULED_TIME:        ${r[9]}")
    log.info("START_TIME:            ${r[10]}")
    log.info("END_TIME:              ${r[11]}")
    log.info("ELAPSED_TIME_MS:       ${r[12]}")
    log.info("INPUT_ROWSETS_COUNT:   ${r[13]}")
    log.info("INPUT_ROW_NUM:         ${r[14]}")
    log.info("INPUT_DATA_SIZE:       ${r[15]}")
    log.info("INPUT_SEGMENTS_NUM:    ${r[16]}")
    log.info("INPUT_VERSION_RANGE:   ${r[17]}")
    log.info("MERGED_ROWS:           ${r[18]}")
    log.info("FILTERED_ROWS:         ${r[19]}")
    log.info("OUTPUT_ROW_NUM:        ${r[20]}")
    log.info("OUTPUT_DATA_SIZE:      ${r[21]}")
    log.info("OUTPUT_SEGMENTS_NUM:   ${r[22]}")
    log.info("OUTPUT_VERSION:        ${r[23]}")
    log.info("BYTES_READ_FROM_LOCAL: ${r[24]}")
    log.info("BYTES_READ_FROM_REMOTE:${r[25]}")
    log.info("PEAK_MEMORY_BYTES:     ${r[26]}")
    log.info("IS_VERTICAL:           ${r[27]}")
    log.info("PERMITS:               ${r[28]}")
    log.info("STATUS_MSG:            ${r[29]}")

    // Verify key columns have reasonable values for a completed compaction
    assertTrue(Long.parseLong(r[0].toString()) > 0, "BACKEND_ID > 0")
    assertTrue(Long.parseLong(r[1].toString()) > 0, "COMPACTION_ID > 0")
    assertTrue(Long.parseLong(r[2].toString()) > 0, "TABLE_ID > 0")
    assertTrue(Long.parseLong(r[3].toString()) > 0, "PARTITION_ID > 0")
    assertEquals(tablet_id, r[4].toString(), "TABLET_ID matches")
    assertEquals("cumulative", r[5].toString())
    assertEquals("FINISHED", r[6].toString())
    assertEquals("MANUAL", r[7].toString())
    assertNotNull(r[9], "SCHEDULED_TIME not null")
    assertNotNull(r[10], "START_TIME not null")
    assertNotNull(r[11], "END_TIME not null")
    assertTrue(Long.parseLong(r[12].toString()) >= 0, "ELAPSED_TIME_MS >= 0")
    assertTrue(Long.parseLong(r[13].toString()) > 0, "INPUT_ROWSETS_COUNT > 0")
    assertTrue(Long.parseLong(r[14].toString()) > 0, "INPUT_ROW_NUM > 0")
    assertTrue(Long.parseLong(r[15].toString()) > 0, "INPUT_DATA_SIZE > 0")
    assertTrue(Long.parseLong(r[16].toString()) >= 0, "INPUT_SEGMENTS_NUM >= 0")
    assertTrue(r[17].toString().length() > 0, "INPUT_VERSION_RANGE not empty")
    assertTrue(Long.parseLong(r[20].toString()) >= 0, "OUTPUT_ROW_NUM >= 0")
    assertTrue(Long.parseLong(r[22].toString()) >= 0, "OUTPUT_SEGMENTS_NUM >= 0")
    assertTrue(r[23].toString().length() > 0, "OUTPUT_VERSION not empty")

    // 9. Test: DESC to verify table schema
    def descResult = sql """ DESC information_schema.be_compaction_tasks """
    log.info("DESC result: ${descResult}")
    assertEquals(30, descResult.size(), "Expected 30 columns in DESC")

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
