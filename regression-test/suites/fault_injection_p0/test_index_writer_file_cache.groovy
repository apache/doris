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


suite("test_index_writer_file_cache_fault_injection", "nonConcurrent") {
    if (!isCloudMode()) {
        return;
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def testTable1 = "test_index_writer_file_cache_fault_injection_1"
    def testTable2 = "test_index_writer_file_cache_fault_injection_2"

    sql "DROP TABLE IF EXISTS ${testTable1}"
    sql """
        CREATE TABLE ${testTable1} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` string NULL COMMENT "",
          `request` string NULL COMMENT "",
          `status` int(11) NULL COMMENT "",
          `size` int(11) NULL COMMENT "",
          INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
          INDEX status_idx (`status`) USING INVERTED COMMENT '',
          INDEX size_idx (`size`) USING INVERTED COMMENT ''
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "disable_auto_compaction" = "true"
        );
    """

    sql "DROP TABLE IF EXISTS ${testTable2}"
    sql """
        CREATE TABLE ${testTable2} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` string NULL COMMENT "",
          `request` string NULL COMMENT "",
          `status` int(11) NULL COMMENT "",
          `size` int(11) NULL COMMENT "",
          INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
          INDEX status_idx (`status`) USING INVERTED COMMENT '',
          INDEX size_idx (`size`) USING INVERTED COMMENT ''
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "disable_auto_compaction" = "true"
        );
    """

    def insert_and_compaction = { tableName ->
      sql """ INSERT INTO ${tableName} VALUES (893964617, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
      sql """ INSERT INTO ${tableName} VALUES (893964653, '232.0.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 3781); """
      sql """ INSERT INTO ${tableName} VALUES (893964672, '26.1.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 304, 0); """

      def tablets = sql_return_maparray """ show tablets from ${tableName}; """

      for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        String backend_id = tablet.BackendId
        def (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        assertEquals("success", compactJson.status.toLowerCase())
      }

      for (def tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet.TabletId
            String backend_id = tablet.BackendId
            def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
      }
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput::init.file_cache")

        insert_and_compaction.call(testTable1);
        insert_and_compaction.call(testTable2);
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput::init.file_cache")
    }
}