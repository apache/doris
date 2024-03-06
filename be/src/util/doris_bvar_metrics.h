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

#pragma once

#include <bthread/mutex.h>
#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>
#include <bvar/status.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "util/bvar_metrics.h"
#include "util/system_bvar_metrics.h"
namespace doris {

#define DORIS_REGISTER_HOOK_METRIC(metric, func)                                     \
    DorisBvarMetrics::instance()->server_entity()->register_metric(#metric, metric); \
    DorisBvarMetrics::instance()->server_entity()->register_hook(                    \
            #metric, [&]() { metric.set_value(func()); });

#define DORIS_DEREGISTER_HOOK_METRIC(name)                                   \
    DorisBvarMetrics::instance()->server_entity()->deregister_metric(#name); \
    DorisBvarMetrics::instance()->server_entity()->deregister_hook(#name);

class DorisBvarMetrics {
public:
    static DorisBvarMetrics* instance() {
        static DorisBvarMetrics metrics;
        return &metrics;
    }

    void initialize(
            bool init_system_metrics = false,
            const std::set<std::string>& disk_devices = std::set<std::string>(),
            const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    BvarMetricRegistry* metric_registry() { return &metric_registry_; }
    SystemBvarMetrics* system_metrics() { return system_metrics_.get(); }
    BvarMetricEntity* server_entity() { return server_metric_entity_.get(); }

private:
    // Don't allow constructor
    DorisBvarMetrics();

    void update();
    void update_process_thread_num();
    void update_process_fd_num();

private:
    static const std::string s_registry_name_;
    static const std::string s_hook_name_;

    BvarMetricRegistry metric_registry_;

    std::unique_ptr<SystemBvarMetrics> system_metrics_;

    std::shared_ptr<BvarMetricEntity> server_metric_entity_;
};

extern BvarAdderMetric<int64_t> g_adder_fragment_requests_total;
extern BvarAdderMetric<int64_t> g_adder_fragment_request_duration_us;
extern BvarAdderMetric<int64_t> g_adder_query_scan_bytes;
extern BvarAdderMetric<int64_t> g_adder_query_scan_rows;

extern BvarAdderMetric<int64_t> g_adder_push_requests_success_total;
extern BvarAdderMetric<int64_t> g_adder_push_requests_fail_total;
extern BvarAdderMetric<int64_t> g_adder_push_request_duration_us;
extern BvarAdderMetric<int64_t> g_adder_push_request_write_bytes;
extern BvarAdderMetric<int64_t> g_adder_push_request_write_rows;

extern BvarAdderMetric<int64_t> g_adder_create_tablet_requests_total;
extern BvarAdderMetric<int64_t> g_adder_create_tablet_requests_failed;
extern BvarAdderMetric<int64_t> g_adder_drop_tablet_requests_total;

extern BvarAdderMetric<int64_t> g_adder_report_all_tablets_requests_skip;

extern BvarAdderMetric<int64_t> g_adder_schema_change_requests_total;
extern BvarAdderMetric<int64_t> g_adder_schema_change_requests_failed;
extern BvarAdderMetric<int64_t> g_adder_create_rollup_requests_total;
extern BvarAdderMetric<int64_t> g_adder_create_rollup_requests_failed;
extern BvarAdderMetric<int64_t> g_adder_storage_migrate_requests_total;
extern BvarAdderMetric<int64_t> g_adder_storage_migrate_v2_requests_total;
extern BvarAdderMetric<int64_t> g_adder_storage_migrate_v2_requests_failed;
extern BvarAdderMetric<int64_t> g_adder_delete_requests_total;
extern BvarAdderMetric<int64_t> g_adder_delete_requests_failed;
extern BvarAdderMetric<int64_t> g_adder_clone_requests_total;
extern BvarAdderMetric<int64_t> g_adder_clone_requests_failed;
extern BvarAdderMetric<int64_t> g_adder_alter_inverted_index_requests_total;
extern BvarAdderMetric<int64_t> g_adder_alter_inverted_index_requests_failed;

extern BvarAdderMetric<int64_t> g_adder_finish_task_requests_total;
extern BvarAdderMetric<int64_t> g_adder_finish_task_requests_failed;

extern BvarAdderMetric<int64_t> g_adder_base_compaction_request_total;
extern BvarAdderMetric<int64_t> g_adder_base_compaction_request_failed;
extern BvarAdderMetric<int64_t> g_adder_cumulative_compaction_request_total;
extern BvarAdderMetric<int64_t> g_adder_cumulative_compaction_request_failed;

extern BvarAdderMetric<int64_t> g_adder_base_compaction_deltas_total;
extern BvarAdderMetric<int64_t> g_adder_base_compaction_bytes_total;
extern BvarAdderMetric<int64_t> g_adder_cumulative_compaction_deltas_total;
extern BvarAdderMetric<int64_t> g_adder_cumulative_compaction_bytes_total;
extern BvarAdderMetric<int64_t> g_adder_full_compaction_deltas_total;
extern BvarAdderMetric<int64_t> g_adder_full_compaction_bytes_total;

extern BvarAdderMetric<int64_t> g_adder_publish_task_request_total;
extern BvarAdderMetric<int64_t> g_adder_publish_task_failed_total;

// Counters for segment_v2
// -----------------------
// total number of segments read
extern BvarAdderMetric<int64_t> g_adder_segment_read_total;
// total number of rows in queried segments (before index pruning)
extern BvarAdderMetric<int64_t> g_adder_segment_row_total;

extern BvarAdderMetric<int64_t> g_adder_stream_load_txn_begin_request_total;
extern BvarAdderMetric<int64_t> g_adder_stream_load_txn_commit_request_total;
extern BvarAdderMetric<int64_t> g_adder_stream_load_txn_rollback_request_total;
extern BvarAdderMetric<int64_t> g_adder_stream_receive_bytes_total;
extern BvarAdderMetric<int64_t> g_adder_stream_load_rows_total;
extern BvarAdderMetric<int64_t> g_adder_load_rows;
extern BvarAdderMetric<int64_t> g_adder_load_bytes;

extern BvarAdderMetric<int64_t> g_adder_memtable_flush_total;
extern BvarAdderMetric<int64_t> g_adder_memtable_flush_duration_us;

extern BvarAdderMetric<int64_t> g_adder_memory_pool_bytes_total;
extern BvarAdderMetric<int64_t> g_adder_process_thread_num;
extern BvarAdderMetric<int64_t> g_adder_process_fd_num_used;
extern BvarAdderMetric<int64_t> g_adder_process_fd_num_limit_soft;
extern BvarAdderMetric<int64_t> g_adder_process_fd_num_limit_hard;

// the max compaction score of all tablets.
// Record base and cumulative scores separately, because
// we need to get the larger of the two.
extern BvarAdderMetric<int64_t> g_adder_tablet_cumulative_max_compaction_score;
extern BvarAdderMetric<int64_t> g_adder_tablet_base_max_compaction_score;

extern BvarAdderMetric<int64_t> g_adder_all_rowsets_num;
extern BvarAdderMetric<int64_t> g_adder_all_segments_num;

// permits have been used for all compaction tasks
extern BvarAdderMetric<int64_t> g_adder_compaction_used_permits;
// permits required by the compaction task which is waiting for permits
extern BvarAdderMetric<int64_t> g_adder_compaction_waitting_permits;

// HistogramMetric* tablet_version_num_distribution;

// The following metrics will be calculated
// by metric calculator
extern BvarAdderMetric<int64_t> g_adder_query_scan_bytes_per_second;

// Metrics related with file reader/writer
extern BvarAdderMetric<int64_t> g_adder_local_file_reader_total;
extern BvarAdderMetric<int64_t> g_adder_s3_file_reader_total;
extern BvarAdderMetric<int64_t> g_adder_hdfs_file_reader_total;
extern BvarAdderMetric<int64_t> g_adder_broker_file_reader_total;
extern BvarAdderMetric<int64_t> g_adder_local_file_writer_total;
extern BvarAdderMetric<int64_t> g_adder_s3_file_writer_total;
extern BvarAdderMetric<int64_t> g_adder_file_created_total;
extern BvarAdderMetric<int64_t> g_adder_s3_file_created_total;
extern BvarAdderMetric<int64_t> g_adder_local_bytes_read_total;
extern BvarAdderMetric<int64_t> g_adder_s3_bytes_read_total;
extern BvarAdderMetric<int64_t> g_adder_local_bytes_written_total;
extern BvarAdderMetric<int64_t> g_adder_s3_bytes_written_total;

extern BvarAdderMetric<int64_t> g_adder_local_file_open_reading;
extern BvarAdderMetric<int64_t> g_adder_s3_file_open_reading;
extern BvarAdderMetric<int64_t> g_adder_hdfs_file_open_reading;
extern BvarAdderMetric<int64_t> g_adder_broker_file_open_reading;
extern BvarAdderMetric<int64_t> g_adder_local_file_open_writing;
extern BvarAdderMetric<int64_t> g_adder_s3_file_open_writing;

// Size of some global containers
extern BvarAdderMetric<uint64_t> g_adder_rowset_count_generated_and_in_use;
extern BvarAdderMetric<uint64_t> g_adder_unused_rowsets_count;
extern BvarAdderMetric<uint64_t> g_adder_broker_count;
extern BvarAdderMetric<uint64_t> g_adder_data_stream_receiver_count;
extern BvarAdderMetric<uint64_t> g_adder_fragment_endpoint_count;
extern BvarAdderMetric<uint64_t> g_adder_active_scan_context_count;
extern BvarAdderMetric<uint64_t> g_adder_fragment_instance_count;
extern BvarAdderMetric<uint64_t> g_adder_load_channel_count;
extern BvarAdderMetric<uint64_t> g_adder_result_buffer_block_count;
extern BvarAdderMetric<uint64_t> g_adder_result_block_queue_count;
extern BvarAdderMetric<uint64_t> g_adder_routine_load_task_count;
extern BvarAdderMetric<uint64_t> g_adder_small_file_cache_count;
extern BvarAdderMetric<uint64_t> g_adder_stream_load_pipe_count;
extern BvarAdderMetric<uint64_t> g_adder_new_stream_load_pipe_count;
extern BvarAdderMetric<uint64_t> g_adder_brpc_endpoint_stub_count;
extern BvarAdderMetric<uint64_t> g_adder_brpc_function_endpoint_stub_count;
extern BvarAdderMetric<uint64_t> g_adder_tablet_writer_count;

extern BvarAdderMetric<uint64_t> g_adder_segcompaction_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_compaction_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_load_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_load_channel_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_memtable_memory_limiter_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_query_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_schema_change_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_storage_migration_mem_consumption;
extern BvarAdderMetric<uint64_t> g_adder_tablet_meta_mem_consumption;

// Cache metrics
extern BvarAdderMetric<uint64_t> g_adder_query_cache_memory_total_byte;
extern BvarAdderMetric<uint64_t> g_adder_query_cache_sql_total_count;
extern BvarAdderMetric<uint64_t> g_adder_query_cache_partition_total_count;

extern BvarAdderMetric<int64_t> g_adder_lru_cache_memory_bytes;

extern BvarAdderMetric<uint64_t> g_adder_scanner_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_add_batch_task_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_send_batch_thread_pool_thread_num;
extern BvarAdderMetric<uint64_t> g_adder_send_batch_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_fragment_thread_pool_queue_size;

// Upload metrics
extern BvarAdderMetric<uint64_t> g_adder_upload_total_byte;
extern BvarAdderMetric<int64_t> g_adder_upload_rowset_count;
extern BvarAdderMetric<int64_t> g_adder_upload_fail_count;

extern BvarAdderMetric<uint64_t> g_adder_light_work_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_heavy_work_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_heavy_work_active_threads;
extern BvarAdderMetric<uint64_t> g_adder_light_work_active_threads;

extern BvarAdderMetric<uint64_t> g_adder_heavy_work_pool_max_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_light_work_pool_max_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_heavy_work_max_threads;
extern BvarAdderMetric<uint64_t> g_adder_light_work_max_threads;

extern BvarAdderMetric<uint64_t> g_adder_flush_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_flush_thread_pool_thread_num;

extern BvarAdderMetric<uint64_t> g_adder_local_scan_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_local_scan_thread_pool_thread_num;
extern BvarAdderMetric<uint64_t> g_adder_remote_scan_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_remote_scan_thread_pool_thread_num;
extern BvarAdderMetric<uint64_t> g_adder_limited_scan_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_limited_scan_thread_pool_thread_num;
extern BvarAdderMetric<uint64_t> g_adder_group_local_scan_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_group_local_scan_thread_pool_thread_num;

extern BvarAdderMetric<uint64_t> g_adder_flush_thread_pool_queue_size;
extern BvarAdderMetric<uint64_t> g_adder_flush_thread_pool_thread_num;

extern BvarAdderMetric<uint64_t> g_adder_memtable_memory_limiter_mem_consumption;

} // namespace doris