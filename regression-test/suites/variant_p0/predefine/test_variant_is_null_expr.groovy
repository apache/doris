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


suite("test_variant_is_null_expr", "p0, nonConcurrent") {
    // define a sql table
    def testTable = "test_variant_is_null_expr"

    sql """ set default_variant_enable_typed_paths_to_sparse = false """

    sql """ DROP TABLE IF EXISTS ${testTable} """ 
    sql """
        CREATE TABLE ${testTable} (
          `k` int(11) NULL COMMENT "",
          `v` variant<
            MATCH_NAME_GLOB 'int*' : int,
            MATCH_NAME_GLOB 'string*' : string
          > NULL COMMENT "",
          INDEX idx_a (v) USING INVERTED PROPERTIES("field_pattern"= "string*", "parser"="unicode", "support_phrase" = "true") COMMENT '',
          INDEX idx_b (v) USING INVERTED PROPERTIES("field_pattern"= "string*"),
          INDEX idx_c (v) USING INVERTED PROPERTIES("field_pattern"= "int*")
          ) ENGINE=OLAP
          DUPLICATE KEY(`k`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`k`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
      """

    sql """
        INSERT INTO ${testTable} VALUES (1, '{"int1" : 1, "string1" : "aa"}'), (2, '{"int2" : 2, "string2" : "bb"}'), (3, '{"int3" : 3, "string3" : "cc"}');
    """


    def queryAndCheck = { String sqlQuery, int expectedFilteredRows = -1, boolean checkFilterUsed = true ->
      def checkpoints_name = "segment_iterator.inverted_index.filtered_rows"
      try {
          GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
          GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [filtered_rows: expectedFilteredRows])
          sql "set experimental_enable_parallel_scan = false"
          sql " set inverted_index_skip_threshold = 0 "
          sql "set enable_common_expr_pushdown = true"
          sql "set enable_common_expr_pushdown_for_inverted_index = true"
          sql "sync"
          sql "${sqlQuery}"
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
          GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
      }
    }
    
    queryAndCheck (" select * from ${testTable} where v['int1'] is not null; ", 2)
    queryAndCheck (" select * from ${testTable} where v['int1'] is null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['string1'] is not null; ", 2)
    queryAndCheck (" select * from ${testTable} where v['string1'] is null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is not null or v['string2'] is not null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is not null or v['string2'] is null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is not null or v['string2'] = 'bb'; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is null or v['string2'] = 'bb'; ", 1)
    queryAndCheck (" select * from ${testTable} where v['string2'] is not null or cast(v['int3'] as int) = 3; ", 1)

    queryAndCheck (" select * from ${testTable} where (v['int1'] is not null and v['string2'] is null) or (v['int1'] is null and v['string2'] = 'bb'); ", 1)
    queryAndCheck (" select * from ${testTable} where (v['int1'] is null and v['string2'] = 'cc') or (v['int3'] is not null and v['string2'] = 'bb'); ", 3)
}
