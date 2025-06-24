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


suite("test_is_null_expr", "p0, nonConcurrent") {
    // define a sql table
    def testTable = "test_is_null_expr"

    sql """ DROP TABLE IF EXISTS ${testTable} """ 
    sql """
        CREATE TABLE ${testTable} (
          `k` int(11) NULL COMMENT "",
          `v` string NULL COMMENT "",
          `v2` int NULL COMMENT "",
          `v3` string NULL COMMENT "",
          INDEX `idx_k`(`k`) using inverted,
          INDEX `idx_v`(`v`) using inverted,
          INDEX `idx_v2`(`v2`) using inverted,
          INDEX `idx_v3`(`v3`) using inverted,
          ) ENGINE=OLAP
          DUPLICATE KEY(`k`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`k`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
      """

    sql """
        INSERT INTO ${testTable} VALUES (1, "a", 1, "a"), (2, "b", 2, "b"), (3, "c", 3, "c");
    """


    def queryAndCheck = { String sqlQuery, int expectedFilteredRows = -1, boolean checkFilterUsed = true ->
      def checkpoints_name = "segment_iterator.inverted_index.filtered_rows"
      try {
          GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
          GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [filtered_rows: expectedFilteredRows])
          sql "set experimental_enable_parallel_scan = false"
          sql " set inverted_index_skip_threshold = 0 "
          sql " set enable_common_expr_pushdown_for_inverted_index = true"
          sql " set enable_common_expr_pushdown = true"
          sql " set enable_parallel_scan = false"
          sql "sync"
          sql "${sqlQuery}"
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
          GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
      }
    }
    
    queryAndCheck (" select * from ${testTable} where v2 is not null; ", 0)
    queryAndCheck (" select * from ${testTable} where v2 is not null or v3 = 'c'; ", 0)
    queryAndCheck (" select * from ${testTable} where v2 is null or v3 = 'c'; ", 2)
    queryAndCheck (" select * from ${testTable} where v2 is not null or v3 is not null; ", 0)
}
