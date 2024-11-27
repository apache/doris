import org.apache.commons.lang3.StringUtils

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

suite("test_nested_type_with_resize") {
    sql """ DROP TABLE IF EXISTS test_array_resize """
    sql """ CREATE TABLE IF NOT EXISTS `test_array_resize` (
              `col1` datetime(4) NOT NULL,
              `col2` smallint NOT NULL,
              `col3` tinyint NOT NULL,
              `col4` array<text> NOT NULL,
              `col15` map<text,text> NOT NULL,
              `col22` array<text> NOT NULL,
              `col25` array<text> NOT NULL,
              `col32` map<text,text> NOT NULL,
              `col57` datetime(4) NOT NULL,
              `col74` struct<f1:varchar(65533),f2:char(32),f3:int,f4:double> NOT NULL,
              `col95` datetime(4) NOT NULL,
              INDEX col95 (`col95`) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(`col1`, `col2`, `col3`)
            DISTRIBUTED BY HASH(`col1`, `col2`, `col3`) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "bloom_filter_columns" = "col57",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            ); """

    streamLoad {
        table "test_array_resize"
        file "test_nested_type_with_resize.csv"

        time 10000 // limit inflight 10s

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(200, json.NumberTotalRows)
            assertEquals(200, json.NumberLoadedRows)
        }
    }

    order_qt_sql """ /*set ShuffleSendBytes=0|ShuffleSendRows=0|FuzzyVariables=batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_instance_num=3,parallel_pipeline_task_num=5,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=true,parallel_scan_max_scanners_count=48,parallel_scan_min_rows_per_scanner=16384,parallel_prepare_threshold=13,enable_fold_constant_by_be=true,enable_rewrite_element_at_to_slot=true,runtime_filter_type=12,enable_parallel_result_sink=true,sort_phase_num=0,rewrite_or_to_in_predicate_threshold=100000,enable_function_pushdown=false,enable_common_expr_pushdown=true,enable_local_exchange=false,partitioned_hash_join_rows_threshold=1048576,partitioned_hash_agg_rows_threshold=1048576,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=false,enable_two_phase_read_opt=true,enable_delete_sub_predicate_v2=false,enable_sort_spill=false,enable_agg_spill=false,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,external_agg_partition_bits=5*/ select col4 from test_array_resize order by col1,col2,col3 limit 10; """
    order_qt_sql """ /*set ShuffleSendBytes=0|ShuffleSendRows=0|FuzzyVariables=batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_instance_num=3,parallel_pipeline_task_num=5,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=true,parallel_scan_max_scanners_count=48,parallel_scan_min_rows_per_scanner=16384,parallel_prepare_threshold=13,enable_fold_constant_by_be=true,enable_rewrite_element_at_to_slot=true,runtime_filter_type=12,enable_parallel_result_sink=true,sort_phase_num=0,rewrite_or_to_in_predicate_threshold=100000,enable_function_pushdown=false,enable_common_expr_pushdown=true,enable_local_exchange=false,partitioned_hash_join_rows_threshold=1048576,partitioned_hash_agg_rows_threshold=1048576,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=false,enable_two_phase_read_opt=true,enable_delete_sub_predicate_v2=false,enable_sort_spill=false,enable_agg_spill=false,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,external_agg_partition_bits=5*/ select col22 from test_array_resize order by col1,col2,col3 limit 10; """
    order_qt_sql """ /*set ShuffleSendBytes=0|ShuffleSendRows=0|FuzzyVariables=batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_instance_num=3,parallel_pipeline_task_num=5,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=true,parallel_scan_max_scanners_count=48,parallel_scan_min_rows_per_scanner=16384,parallel_prepare_threshold=13,enable_fold_constant_by_be=true,enable_rewrite_element_at_to_slot=true,runtime_filter_type=12,enable_parallel_result_sink=true,sort_phase_num=0,rewrite_or_to_in_predicate_threshold=100000,enable_function_pushdown=false,enable_common_expr_pushdown=true,enable_local_exchange=false,partitioned_hash_join_rows_threshold=1048576,partitioned_hash_agg_rows_threshold=1048576,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=false,enable_two_phase_read_opt=true,enable_delete_sub_predicate_v2=false,enable_sort_spill=false,enable_agg_spill=false,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,external_agg_partition_bits=5*/ select col32 from test_array_resize order by col1,col2,col3 limit 10; """
    order_qt_sql """ /*set ShuffleSendBytes=0|ShuffleSendRows=0|FuzzyVariables=batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_instance_num=3,parallel_pipeline_task_num=5,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=true,parallel_scan_max_scanners_count=48,parallel_scan_min_rows_per_scanner=16384,parallel_prepare_threshold=13,enable_fold_constant_by_be=true,enable_rewrite_element_at_to_slot=true,runtime_filter_type=12,enable_parallel_result_sink=true,sort_phase_num=0,rewrite_or_to_in_predicate_threshold=100000,enable_function_pushdown=false,enable_common_expr_pushdown=true,enable_local_exchange=false,partitioned_hash_join_rows_threshold=1048576,partitioned_hash_agg_rows_threshold=1048576,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=false,enable_two_phase_read_opt=true,enable_delete_sub_predicate_v2=false,enable_sort_spill=false,enable_agg_spill=false,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,external_agg_partition_bits=5*/ select col74 from test_array_resize order by col1,col2,col3 limit 10; """

}
