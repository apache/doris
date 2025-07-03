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

suite("test_index_empty_segment_fault_injection", "nonConcurrent") {
    // define a sql table
    def indexTbName = "test_index_empty_segment_fault_injection"

    sql "DROP TABLE IF EXISTS ${indexTbName}"
    sql """
      CREATE TABLE ${indexTbName} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
        INDEX status_idx (`status`) USING INVERTED COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """

    try {
      GetDebugPoint().enableDebugPointForAllBEs("inverted_index_writer.add_document")

      sql """ INSERT INTO ${indexTbName} VALUES (893964617, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """

      sql "sync"

      sql """ set enable_common_expr_pushdown = true; """
      sql """ set enable_match_without_inverted_index = false; """

      try {
          sql """ select * from ${indexTbName} where request match 'hm'; """
      } catch (Exception e) {
          logger.error("SQL execution failed: " + e.getMessage())
          assertTrue(e.getMessage().contains("execute_match"))
      }
    } finally {
      GetDebugPoint().disableDebugPointForAllBEs("inverted_index_writer.add_document")
    }
}