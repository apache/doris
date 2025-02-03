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

suite("test_storage_page_size_fault", "nonConcurrent") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    boolean disableAutoCompaction = false
  
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def dbName = "regression_test_fault_injection_p0"
    def tableName = "test_storage_page_size_fault"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
      CREATE TABLE ${tableName} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT ""
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true",
        "storage_page_size" = "65537"
      );
    """

    def tableId = getTableId(dbName, tableName)
    if (tableId == null) {
      throw new IllegalStateException("Table ID not found for table: ${tableName}")
    }
    logger.info("tableId: " + tableId)

    try {
      GetDebugPoint().enableDebugPointForAllBEs("VerticalSegmentWriter._create_column_writer.storage_page_size", ["table_id": tableId, "storage_page_size": 69632])
      sql """ INSERT INTO ${tableName} VALUES (893964617, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """

      set_be_config.call("enable_vertical_segment_writer", "false")
      sql """ INSERT INTO ${tableName} VALUES (893964617, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
      set_be_config.call("enable_vertical_segment_writer", "true")

    } finally {
      GetDebugPoint().disableDebugPointForAllBEs("VerticalSegmentWriter._create_column_writer.storage_page_size")
    }
}