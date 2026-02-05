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


suite("test_float_double", "p0, nonConcurrent"){
    def tableName = "test_float_double"

    sql """ set describe_extend_variant_column = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set inverted_index_skip_threshold = 0 """

    def queryAndCheck = { String sqlQuery, int expectedFilteredRows = -1, boolean checkFilterUsed = true ->
      def checkpoints_name = "segment_iterator.inverted_index.filtered_rows"
      try {
          GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
          GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [filtered_rows: expectedFilteredRows])
          sql "set experimental_enable_parallel_scan = false"
          sql "sync"
          sql "${sqlQuery}"
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
          GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
      }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `float_col` float NULL,
            `double_col` double NULL,
            INDEX idx_float_col (float_col) USING INVERTED,
            INDEX idx_double_col (double_col) USING INVERTED
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")
    """
    
    sql """ insert into ${tableName} values (1, 1.5, 1.5239849328948), (2, 2.23, 2.239849328948), (3, 3.02, 3.029849328948) """


    queryAndCheck("select count() from ${tableName} where double_col = 1.5239849328948", 2)
    queryAndCheck("select count() from ${tableName} where double_col = 2.239849328948", 2)
    queryAndCheck("select count() from ${tableName} where double_col = 3.029849328948", 2)


    queryAndCheck("select count() from ${tableName} where float_col = cast(1.5 as float)", 2)
    queryAndCheck("select count() from ${tableName} where float_col = cast(2.23 as float)", 2)
    queryAndCheck("select count() from ${tableName} where float_col = cast(3.02 as float)", 2)
    
}
