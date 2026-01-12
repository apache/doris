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
    sql "set default_variant_max_subcolumns_count = 10"
    sql """ DROP TABLE IF EXISTS ${testTable} """ 
    sql """
        CREATE TABLE ${testTable} (
          `k` int(11) NULL COMMENT "",
          `v` variant NULL COMMENT "",
          INDEX idx_a (v) USING INVERTED
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
    
    queryAndCheck (" select /*+ SET_VAR(batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_pipeline_task_num=1,enable_binary_search_filtering_partitions=true,enable_sql_cache=false,enable_parallel_scan=true,parallel_scan_max_scanners_count=48,parallel_scan_min_rows_per_scanner=2097152,use_serial_exchange=false,enable_shared_exchange_sink_buffer=true,parallel_prepare_threshold=2,enable_fold_constant_by_be=false,enable_rewrite_element_at_to_slot=true,runtime_filter_wait_infinitely=true,runtime_filter_type=5,runtime_filter_max_in_num=40960,enable_sync_runtime_filter_size=false,enable_parallel_result_sink=true,sort_phase_num=0,rewrite_or_to_in_predicate_threshold=2,enable_runtime_filter_prune=false,enable_runtime_filter_partition_prune=false,enable_fast_analyze_into_values=true,enable_function_pushdown=true,enable_common_expr_pushdown=true,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=true,enable_two_phase_read_opt=true,enable_common_expr_pushdown_for_inverted_index=false,fe_debug=true,fetch_remote_schema_timeout_seconds=120,max_fetch_remote_schema_tablet_count=512,data_queue_max_blocks=1,enable_spill=true,enable_reserve_memory=true,spill_min_revocable_mem=104857600,spill_aggregation_partition_count=32,spill_hash_join_partition_count=32,spill_revocable_memory_high_watermark_percent=-1) */ * from ${testTable} where v['int1'] is not null; ", 2)
    queryAndCheck (" select * from ${testTable} where v['int1'] is null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['string1'] is not null; ", 2)
    queryAndCheck (" select * from ${testTable} where v['string1'] is null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is not null or v['string2'] is not null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is not null or v['string2'] is null; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is not null or v['string2'] = 'bb'; ", 1)
    queryAndCheck (" select * from ${testTable} where v['int1'] is null or v['string2'] = 'bb'; ", 1)
    queryAndCheck (" select * from ${testTable} where v['string2'] is not null or cast(v['int3'] as tinyint) = 3; ", 1)

    queryAndCheck (" select * from ${testTable} where cast(v['int1'] as tinyint) is not null or cast(v['string2'] as string) = 'bb'; ", 1)
    queryAndCheck (" select * from ${testTable} where cast(v['int1'] as tinyint) is null or cast(v['string2'] as string) = 'bb'; ", 1)
    queryAndCheck (" select * from ${testTable} where cast(v['string2'] as string) is not null or cast(v['int3'] as tinyint) = 3; ", 1)

    queryAndCheck (" select * from ${testTable} where (v['int1'] is not null and v['string2'] is null) or (v['int1'] is null and v['string2'] = 'bb'); ", 1)
    queryAndCheck (" select * from ${testTable} where (v['int1'] is null and v['string2'] = 'cc') or (v['int3'] is not null and v['string2'] = 'bb'); ", 3)

    queryAndCheck (" select * from ${testTable} where (cast(v['int1'] as tinyint) is not null and cast(v['string2'] as string) is null) or (cast(v['int1'] as tinyint) is null and cast(v['string2'] as string) = 'bb'); ", 1)
    queryAndCheck (" select * from ${testTable} where (cast(v['int1'] as tinyint) is null and cast(v['string2'] as string) = 'cc') or (cast(v['int3'] as tinyint) is not null and cast(v['string2'] as string) = 'bb'); ", 3)
}
