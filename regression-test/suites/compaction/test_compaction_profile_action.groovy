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

suite("test_compaction_profile_action") {
    def tableName = "test_compaction_profile_action"

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

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    def tablets = sql_return_maparray """ show tablets from ${tableName} """
    def tablet = tablets[0]
    def tablet_id = tablet.TabletId
    def be_host = backendId_to_backendIP["${tablet.BackendId}"]
    def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]
    def beHttpAddress = "${be_host}:${be_port}"

    // 1. Test: API returns valid JSON with empty or existing profiles
    def (code1, text1, err1) = curl("GET", "${beHttpAddress}/api/compaction/profile")
    assertEquals(0, code1)
    def resp1 = parseJson(text1.trim())
    assertEquals("Success", resp1.status)
    assertNotNull(resp1.compaction_profiles)
    def profileCountBefore = resp1.compaction_profiles.size()
    log.info("Profile count before compaction: ${profileCountBefore}")

    // 2. Trigger a cumulative compaction
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

    // 3. Test: New profile record should appear
    def (code3, text3, err3) = curl("GET", "${beHttpAddress}/api/compaction/profile?tablet_id=${tablet_id}")
    assertEquals(0, code3)
    def resp3 = parseJson(text3.trim())
    assertEquals("Success", resp3.status)
    assertTrue(resp3.compaction_profiles.size() > 0, "Expected at least one profile record after compaction")

    def latestProfile = resp3.compaction_profiles[0]
    log.info("Latest profile: ${latestProfile}")

    // Verify key fields exist and have reasonable values
    assertNotNull(latestProfile.compaction_id)
    assertNotNull(latestProfile.compaction_type)
    assertEquals(Long.parseLong(tablet_id), latestProfile.tablet_id)
    assertNotNull(latestProfile.start_time)
    assertNotNull(latestProfile.end_time)
    assertTrue(latestProfile.cost_time_ms >= 0)
    assertTrue(latestProfile.success)
    assertTrue(latestProfile.input_rowsets_count > 0)
    assertTrue(latestProfile.input_row_num > 0)
    assertTrue(latestProfile.input_rowsets_total_size > 0)
    assertNotNull(latestProfile.output_version)

    // 4. Test: top_n parameter
    def (code4, text4, err4) = curl("GET", "${beHttpAddress}/api/compaction/profile?top_n=1")
    assertEquals(0, code4)
    def resp4 = parseJson(text4.trim())
    assertTrue(resp4.compaction_profiles.size() <= 1)

    // 5. Test: invalid top_n returns 400
    def (code5, text5, err5) = curl("GET", "${beHttpAddress}/api/compaction/profile?top_n=-1")
    assertTrue(text5.contains("top_n must be non-negative"))

    // 6. Test: non-existent tablet_id returns empty list
    def (code6, text6, err6) = curl("GET", "${beHttpAddress}/api/compaction/profile?tablet_id=999999999")
    assertEquals(0, code6)
    def resp6 = parseJson(text6.trim())
    assertEquals(0, resp6.compaction_profiles.size())

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
