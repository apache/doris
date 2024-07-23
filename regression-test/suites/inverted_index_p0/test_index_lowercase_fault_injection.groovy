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


suite("test_index_lowercase_fault_injection", "nonConcurrent") {
    // define a sql table
    def testTable = "httplogs_lowercase"

    def create_httplogs_unique_table = {testTablex ->
      // multi-line sql
      def result = sql """
        CREATE TABLE ${testTablex} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` string NULL COMMENT "",
          `request` string NULL COMMENT "",
          `status` string NULL COMMENT "",
          `size` string NULL COMMENT "",
          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "chinese", "support_phrase" = "true") COMMENT ''
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "compaction_policy" = "time_series",
          "time_series_compaction_goal_size_mbytes" = "1024",
          "time_series_compaction_file_count_threshold" = "20",
          "time_series_compaction_time_threshold_seconds" = "3600"
        );
      """
    }

    try {
      sql "DROP TABLE IF EXISTS ${testTable}"
      create_httplogs_unique_table.call(testTable)

      try {
        GetDebugPoint().enableDebugPointForAllBEs("inverted_index_parser.get_parser_lowercase_from_properties")
        GetDebugPoint().enableDebugPointForAllBEs("tablet_schema.to_schema_pb")

        sql """ INSERT INTO ${testTable} VALUES (893964617, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
        sql """ INSERT INTO ${testTable} VALUES (893964653, '232.0.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 3781); """
        sql """ INSERT INTO ${testTable} VALUES (893964672, '26.1.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 304, 0); """
        sql """ INSERT INTO ${testTable} VALUES (893964672, '26.1.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 304, 0); """
        sql """ INSERT INTO ${testTable} VALUES (893964653, '232.0.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 3781); """

        sql 'sync'

        qt_sql """ select count() from ${testTable} where (request match 'HTTP');  """
        qt_sql """ select count() from ${testTable} where (request match 'http');  """
      } finally {
        GetDebugPoint().disableDebugPointForAllBEs("inverted_index_parser.get_parser_lowercase_from_properties")
        GetDebugPoint().disableDebugPointForAllBEs("tablet_schema.to_schema_pb")
      }

      sql """ INSERT INTO ${testTable} VALUES (893964672, '26.1.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 304, 0); """
      sql """ INSERT INTO ${testTable} VALUES (893964672, '26.1.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 304, 0); """
      sql """ INSERT INTO ${testTable} VALUES (893964653, '232.0.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 3781); """

      sql 'sync'

      qt_sql """ select count() from ${testTable} where (request match 'HTTP');  """
      qt_sql """ select count() from ${testTable} where (request match 'http');  """
    } finally {
    }
 }
