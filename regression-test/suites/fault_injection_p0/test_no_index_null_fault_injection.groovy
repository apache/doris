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


suite("test_no_index_null", "nonConcurrent") {
    // define a sql table
    def testTable = "test_no_index_null"

    def create_httplogs_unique_table = {testTablex ->
      // multi-line sql
      def result = sql """
        CREATE TABLE ${testTablex} (
          `@timestamp` INT NULL,
          `clientip` VARCHAR(20) NULL,
          `request` TEXT NULL,
          `status` INT NULL,
          `size` INT NULL,
          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true", "lower_case" = "true") COMMENT ''''
        ) ENGINE=OLAP
        DUPLICATE KEY(`@timestamp`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
        );
      """
    }

    try {
      sql "DROP TABLE IF EXISTS ${testTable}"
      create_httplogs_unique_table.call(testTable)
      
      try {
          GetDebugPoint().enableDebugPointForAllBEs("column_writer.init")
          
          sql """ INSERT INTO ${testTable} VALUES (1, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
          sql """ INSERT INTO ${testTable} VALUES (1, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
          sql """ INSERT INTO ${testTable} VALUES (1, '40.135.0.0', NULL, 200, 24736); """
          sql """ INSERT INTO ${testTable} VALUES (1, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
          sql """ INSERT INTO ${testTable} VALUES (1, '40.135.0.0', NULL, 200, 24736); """
          sql 'sync'

          qt_sql """ select count() from ${testTable} where request IS NULL;  """
          qt_sql """ select count() from ${testTable} where request IS NOT NULL;  """
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs("column_writer.init")
      }
    } finally {
    }
}