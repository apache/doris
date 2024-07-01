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

suite("statistic_table_compaction", "nonConcurrent,p0") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def do_compaction = { String table ->
        try {
            def tablets = sql_return_maparray """show tablets from ${table}"""

            // trigger compactions for all tablets in ${tableName}
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                String backend_id = tablet.BackendId
                def (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactJson = parseJson(out.trim())
                assertEquals("success", compactJson.status.toLowerCase())
            }

            Integer counter = 600

            // wait for all compactions done
            for (def tablet in tablets) {
                boolean running = true
                do {
                    counter -= 1
                    Thread.sleep(1000)
                    String tablet_id = tablet.TabletId
                    String backend_id = tablet.BackendId
                    def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    running = compactionStatus.run_status
                } while (running && counter > 0)
            }

            assertTrue(counter >= 0)
        } catch (Exception e) {
            logger.info(e.getMessage())
            if (e.getMessage().contains("Unknown table")) {
                return
            } else {
                throw e
            }
        }
    }

    do_compaction("__internal_schema.column_statistics")
    do_compaction("__internal_schema.partition_statistics")
}
