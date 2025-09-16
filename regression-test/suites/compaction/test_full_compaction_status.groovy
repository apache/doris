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

suite("test_full_compaction_status") {
    def tableName = "test_full_compaction_status"

    try {
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
        
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
            `user_id` INT NOT NULL, 
            `value` INT NOT NULL)
            UNIQUE KEY(`user_id`) 
            DISTRIBUTED BY HASH(`user_id`) 
            BUCKETS 1 
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true"
            );"""

        sql """ INSERT INTO ${tableName} VALUES (1,1),(2,2) """
        sql """ INSERT INTO ${tableName} VALUES (1,10),(2,20) """
        sql """ INSERT INTO ${tableName} VALUES (1,100),(2,200) """

        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Initial compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            assert tabletJson.rowsets.size() > 1

            assertTrue(out.contains("last cumulative failure time"))
            assertTrue(out.contains("last base failure time"))
            assertTrue(out.contains("last full failure time"))
            assertTrue(out.contains("last cumulative success time"))
            assertTrue(out.contains("last base success time"))
            assertTrue(out.contains("last full success time"))
            assertTrue(out.contains("last cumulative schedule time"))
            assertTrue(out.contains("last base schedule time"))
            assertTrue(out.contains("last full schedule time"))
            
            assertTrue(out.contains("last cumulative status"))
            assertTrue(out.contains("last base status"))
            assertTrue(out.contains("last full status"))

            assertTrue(out.contains("\"last cumulative failure time\": \"1970-01-01"))
            assertTrue(out.contains("\"last base failure time\": \"1970-01-01"))
            assertTrue(out.contains("\"last full failure time\": \"1970-01-01"))
            assertTrue(out.contains("\"last cumulative success time\": \"1970-01-01"))
            assertTrue(out.contains("\"last base success time\": \"1970-01-01"))
            assertTrue(out.contains("\"last full success time\": \"1970-01-01"))
            assertTrue(out.contains("\"last cumulative schedule time\": \"1970-01-01"))
            assertTrue(out.contains("\"last base schedule time\": \"1970-01-01"))
            assertTrue(out.contains("\"last full schedule time\": \"1970-01-01"))
        }

        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            String backend_id = tablet.BackendId
            
            def (code, out, err) = be_run_full_compaction(
                backendId_to_backendIP.get(backend_id), 
                backendId_to_backendHttpPort.get(backend_id), 
                tablet_id
            )
            logger.info("Trigger full compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def triggerJson = parseJson(out.trim())
            assertEquals("success", triggerJson.status.toLowerCase())

            boolean running = true
            int checkCount = 0
            while (running && checkCount < 10) {
                Thread.sleep(1000)
                (code, out, err) = be_get_compaction_status(
                    backendId_to_backendIP.get(backend_id),
                    backendId_to_backendHttpPort.get(backend_id),
                    tablet_id
                )
                logger.info("Check compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def statusJson = parseJson(out.trim())
                assertEquals("success", statusJson.status.toLowerCase())
                running = statusJson.run_status
                checkCount++
            }
            assert checkCount < 10, "Full compaction didn't complete within expected time"

            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Detailed compaction status: code=" + code + ", out=" + out + ", err=" + err)
            
            assertTrue(out.contains("last cumulative failure time"))
            assertTrue(out.contains("last base failure time"))
            assertTrue(out.contains("last full failure time"))
            assertTrue(out.contains("last cumulative success time"))
            assertTrue(out.contains("last base success time"))
            assertTrue(out.contains("last full success time"))
            assertTrue(out.contains("last cumulative schedule time"))
            assertTrue(out.contains("last base schedule time"))
            assertTrue(out.contains("last full schedule time"))
            
            assertTrue(out.contains("last cumulative status"))
            assertTrue(out.contains("last base status"))
            assertTrue(out.contains("last full status"))
            
            assertTrue(out.contains("last full status\": \"[OK]\""))
            assertFalse(out.contains("last full success time\": \"1970-01-01"))
            assertFalse(out.contains("last full schedule time\": \"1970-01-01"))
        }

        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Final compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            
            def cloudMode = isCloudMode()
            if (cloudMode) {
                assert tabletJson.rowsets.size() == 2
            } else {
                assert tabletJson.rowsets.size() == 1
            }
        }

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}