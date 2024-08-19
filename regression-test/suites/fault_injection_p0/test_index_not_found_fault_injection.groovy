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


suite("test_index_not_found_fault_injection", "nonConcurrent") {
    def testTable = "test_index_not_found"

    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE ${testTable} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` string NULL COMMENT "",
          `request` string NULL COMMENT "",
          `status` string NULL COMMENT "",
          `size` string NULL COMMENT "",
          INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
          INDEX status_idx (`status`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
          INDEX size_idx (`size`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT ''
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "disable_auto_compaction" = "true"
        );
        """

    sql """ INSERT INTO ${testTable} VALUES (893964617, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
    sql """ INSERT INTO ${testTable} VALUES (893964653, '232.0.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 3781); """
    sql """ INSERT INTO ${testTable} VALUES (893964672, '26.1.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 304, 0); """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("inverted file read error: index file not found")

        qt_sql """ select count() from ${testTable} where (request match_phrase 'http');  """
        qt_sql """ select count() from ${testTable} where (request match_phrase_prefix 'http');  """

        qt_sql """ select count() from ${testTable} where (clientip match_phrase 'http' or request match_phrase 'http' or status match_phrase 'http' or size match_phrase 'http');  """
        qt_sql """ select count() from ${testTable} where (clientip match_phrase_prefix 'http' or request match_phrase_prefix 'http' or status match_phrase_prefix 'http' or size match_phrase_prefix 'http');  """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("inverted file read error: index file not found")
    }
}