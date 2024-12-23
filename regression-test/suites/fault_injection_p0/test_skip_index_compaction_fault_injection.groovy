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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_skip_index_compaction_fault_injection", "nonConcurrent") {
  def isCloudMode = isCloudMode()
  def tableName1 = "test_skip_index_compaction_fault_injection_1"
  def tableName2 = "test_skip_index_compaction_fault_injection_2"
  def backendId_to_backendIP = [:]
  def backendId_to_backendHttpPort = [:]
  getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

  sql "DROP TABLE IF EXISTS ${tableName1}"
  sql """
    CREATE TABLE ${tableName1} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT "",
      INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
      INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`@timestamp`)
    COMMENT "OLAP"
    DISTRIBUTED BY RANDOM BUCKETS 1
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "disable_auto_compaction" = "true",
      "inverted_index_storage_format" = "V1"
    );
  """

  sql "DROP TABLE IF EXISTS ${tableName2}"
  sql """
    CREATE TABLE ${tableName2} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT "",
      INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
      INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`@timestamp`)
    COMMENT "OLAP"
    DISTRIBUTED BY RANDOM BUCKETS 1
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "disable_auto_compaction" = "true",
      "inverted_index_storage_format" = "V2"
    );
  """

  boolean disableAutoCompaction = false
  
  def set_be_config = { key, value ->
    for (String backend_id: backendId_to_backendIP.keySet()) {
      def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
      logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }
  }

  def trigger_full_compaction_on_tablets = { tablets ->
    for (def tablet : tablets) {
      String tablet_id = tablet.TabletId
      String backend_id = tablet.BackendId
      int times = 1

      String compactionStatus;
      do{
        def (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        ++times
        sleep(2000)
        compactionStatus = parseJson(out.trim()).status.toLowerCase();
      } while (compactionStatus!="success" && times<=10 && compactionStatus!="e-6010")


      if (compactionStatus == "fail") {
        assertEquals(disableAutoCompaction, false)
        logger.info("Compaction was done automatically!")
      }
      if (disableAutoCompaction && compactionStatus!="e-6010") {
        assertEquals("success", compactionStatus)
      }
    }
  }

  def wait_full_compaction_done = { tablets ->
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

  def get_rowset_count = { tablets ->
    int rowsetCount = 0
    for (def tablet in tablets) {
      def (code, out, err) = curl("GET", tablet.CompactionStatus)
      logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
      assertEquals(code, 0)
      def tabletJson = parseJson(out.trim())
      assert tabletJson.rowsets instanceof List
      rowsetCount +=((List<String>) tabletJson.rowsets).size()
    }
    return rowsetCount
  }

  def check_config = { String key, String value ->
    for (String backend_id: backendId_to_backendIP.keySet()) {
      def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
      logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
      assertEquals(code, 0)
      def configList = parseJson(out.trim())
      assert configList instanceof List
      for (Object ele in (List) configList) {
        assert ele instanceof List<String>
        if (((List<String>) ele)[0] == key) {
          assertEquals(value, ((List<String>) ele)[2])
        }
      }
    }
  }

  def run_test = { tableName -> 
    sql """ INSERT INTO ${tableName} VALUES (1, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (2, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (3, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (4, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (5, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (6, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (7, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (8, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (9, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    sql """ INSERT INTO ${tableName} VALUES (10, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """

    sql "sync"

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    logger.info("tablets: {}", tablets)

    int replicaNum = 1
    def dedup_tablets = deduplicate_tablets(tablets)
    if (dedup_tablets.size() > 0) {
      replicaNum = Math.round(tablets.size() / dedup_tablets.size())
      if (replicaNum != 1 && replicaNum != 3) {
        assert(false)
      }
    }

    int rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 11 * replicaNum)

    // first
    trigger_full_compaction_on_tablets.call(tablets)
    wait_full_compaction_done.call(tablets)

    rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 11 * replicaNum)

    // second
    trigger_full_compaction_on_tablets.call(tablets)
    wait_full_compaction_done.call(tablets)

    rowsetCount = get_rowset_count.call(tablets);
    if (isCloudMode) {
      assert (rowsetCount == (1 + 1) * replicaNum)
    } else {
      assert (rowsetCount == 1 * replicaNum)
    }
  }

  boolean invertedIndexCompactionEnable = false
  boolean has_update_be_config = false
  try {
    String backend_id;
    backend_id = backendId_to_backendIP.keySet()[0]
    def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
    
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList = parseJson(out.trim())
    assert configList instanceof List

    for (Object ele in (List) configList) {
      assert ele instanceof List<String>
      if (((List<String>) ele)[0] == "inverted_index_compaction_enable") {
        invertedIndexCompactionEnable = Boolean.parseBoolean(((List<String>) ele)[2])
        logger.info("inverted_index_compaction_enable: ${((List<String>) ele)[2]}")
      }
    }
    set_be_config.call("inverted_index_compaction_enable", "true")
    has_update_be_config = true
    check_config.call("inverted_index_compaction_enable", "true");

    try {
      GetDebugPoint().enableDebugPointForAllBEs("Compaction::open_inverted_index_file_reader")
      run_test.call(tableName1)
    } finally {
      GetDebugPoint().disableDebugPointForAllBEs("Compaction::open_inverted_index_file_reader")
    }

    // try {
    //   GetDebugPoint().enableDebugPointForAllBEs("Compaction::open_inverted_index_file_writer")
    //   run_test.call(tableName2)
    // } finally {
    //   GetDebugPoint().disableDebugPointForAllBEs("Compaction::open_inverted_index_file_writer")
    // }
  } finally {
    if (has_update_be_config) {
      set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
    }
  }
}