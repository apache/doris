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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "util/bvar_metrics.h"
#include "util/system_bvar_metrics.h"
namespace doris {

class DorisBvarMetrics {
public:
    std::shared_ptr<BvarAdderMetric<int64_t>> fragment_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> fragment_request_duration_us;
    std::shared_ptr<BvarAdderMetric<int64_t>> query_scan_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> query_scan_rows;

    std::shared_ptr<BvarAdderMetric<int64_t>> push_requests_success_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_requests_fail_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_request_duration_us;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_request_write_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> push_request_write_rows;

    std::shared_ptr<BvarAdderMetric<int64_t>> create_tablet_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> create_tablet_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> drop_tablet_requests_total;

    std::shared_ptr<BvarAdderMetric<int64_t>> report_all_tablets_requests_skip;

    std::shared_ptr<BvarAdderMetric<int64_t>> schema_change_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> schema_change_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> create_rollup_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> create_rollup_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> storage_migrate_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> storage_migrate_v2_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> storage_migrate_v2_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> delete_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> delete_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> clone_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> clone_requests_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> alter_inverted_index_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> alter_inverted_index_requests_failed;

    std::shared_ptr<BvarAdderMetric<int64_t>> finish_task_requests_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> finish_task_requests_failed;

    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_request_failed;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_request_failed;

    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_deltas_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> base_compaction_bytes_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_deltas_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> cumulative_compaction_bytes_total;

    std::shared_ptr<BvarAdderMetric<int64_t>> publish_task_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> publish_task_failed_total;

    // Counters for segment_v2
    // -----------------------
    // total number of segments read
    std::shared_ptr<BvarAdderMetric<int64_t>> segment_read_total;
    // total number of rows in queried segments (before index pruning)
    std::shared_ptr<BvarAdderMetric<int64_t>> segment_row_total;

    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_txn_begin_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_txn_commit_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_txn_rollback_request_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_receive_bytes_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> stream_load_rows_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> load_rows;
    std::shared_ptr<BvarAdderMetric<int64_t>> load_bytes;

    std::shared_ptr<BvarAdderMetric<int64_t>> memtable_flush_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> memtable_flush_duration_us;

    std::shared_ptr<BvarAdderMetric<int64_t>> memory_pool_bytes_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_thread_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_fd_num_used;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_fd_num_limit_soft;
    std::shared_ptr<BvarAdderMetric<int64_t>> process_fd_num_limit_hard;

    // the max compaction score of all tablets.
    // Record base and cumulative scores separately, because
    // we need to get the larger of the two.
    std::shared_ptr<BvarAdderMetric<int64_t>> tablet_cumulative_max_compaction_score;
    std::shared_ptr<BvarAdderMetric<int64_t>> tablet_base_max_compaction_score;

    std::shared_ptr<BvarAdderMetric<int64_t>> all_rowsets_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> all_segments_num;

    // permits have been used for all compaction tasks
    std::shared_ptr<BvarAdderMetric<int64_t>> compaction_used_permits;
    // permits required by the compaction task which is waiting for permits
    std::shared_ptr<BvarAdderMetric<int64_t>> compaction_waitting_permits;

    // HistogramMetric* tablet_version_num_distribution;

    // The following metrics will be calculated
    // by metric calculator
    std::shared_ptr<BvarAdderMetric<int64_t>> query_scan_bytes_per_second;

    // Metrics related with file reader/writer
    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> hdfs_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> broker_file_reader_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_writer_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_writer_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> file_created_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_created_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_bytes_read_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_bytes_read_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_bytes_written_total;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_bytes_written_total;

    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> hdfs_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> broker_file_open_reading;
    std::shared_ptr<BvarAdderMetric<int64_t>> local_file_open_writing;
    std::shared_ptr<BvarAdderMetric<int64_t>> s3_file_open_writing;

    // Size of some global containers
    std::shared_ptr<BvarAdderMetric<uint64_t>> rowset_count_generated_and_in_use;
    std::shared_ptr<BvarAdderMetric<uint64_t>> unused_rowsets_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> broker_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> data_stream_receiver_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> fragment_endpoint_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> active_scan_context_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> fragment_instance_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> load_channel_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> result_buffer_block_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> result_block_queue_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> routine_load_task_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> small_file_cache_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> stream_load_pipe_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> new_stream_load_pipe_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> brpc_endpoint_stub_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> brpc_function_endpoint_stub_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> tablet_writer_count;

    std::shared_ptr<BvarAdderMetric<uint64_t>> segcompaction_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> compaction_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> load_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> load_channel_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> memtable_memory_limiter_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> query_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> schema_change_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> storage_migration_mem_consumption;
    std::shared_ptr<BvarAdderMetric<uint64_t>> tablet_meta_mem_consumption;

    // Cache metrics
    std::shared_ptr<BvarAdderMetric<uint64_t>> query_cache_memory_total_byte;
    std::shared_ptr<BvarAdderMetric<uint64_t>> query_cache_sql_total_count;
    std::shared_ptr<BvarAdderMetric<uint64_t>> query_cache_partition_total_count;

    std::shared_ptr<BvarAdderMetric<int64_t>> lru_cache_memory_bytes;

    std::shared_ptr<BvarAdderMetric<uint64_t>> scanner_thread_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> add_batch_task_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> send_batch_thread_pool_thread_num;
    std::shared_ptr<BvarAdderMetric<uint64_t>> send_batch_thread_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> fragment_thread_pool_queue_size;

    // Upload metrics
    std::shared_ptr<BvarAdderMetric<uint64_t>> upload_total_byte;
    std::shared_ptr<BvarAdderMetric<int64_t>> upload_rowset_count;
    std::shared_ptr<BvarAdderMetric<int64_t>> upload_fail_count;

    std::shared_ptr<BvarAdderMetric<uint64_t>> light_work_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> heavy_work_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> heavy_work_active_threads;
    std::shared_ptr<BvarAdderMetric<uint64_t>> light_work_active_threads;

    std::shared_ptr<BvarAdderMetric<uint64_t>> heavy_work_pool_max_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> light_work_pool_max_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> heavy_work_max_threads;
    std::shared_ptr<BvarAdderMetric<uint64_t>> light_work_max_threads;

    std::shared_ptr<BvarAdderMetric<uint64_t>> flush_thread_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> flush_thread_pool_thread_num;

    std::shared_ptr<BvarAdderMetric<uint64_t>> local_scan_thread_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> local_scan_thread_pool_thread_num;
    std::shared_ptr<BvarAdderMetric<uint64_t>> remote_scan_thread_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> remote_scan_thread_pool_thread_num;
    std::shared_ptr<BvarAdderMetric<uint64_t>> limited_scan_thread_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> limited_scan_thread_pool_thread_num;
    std::shared_ptr<BvarAdderMetric<uint64_t>> group_local_scan_thread_pool_queue_size;
    std::shared_ptr<BvarAdderMetric<uint64_t>> group_local_scan_thread_pool_thread_num;

    static DorisBvarMetrics* instance() {
        static DorisBvarMetrics metrics;
        return &metrics;
    }

    void initialize(
            bool init_system_metrics = false,
            const std::set<std::string>& disk_devices = std::set<std::string>(),
            const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    void register_entity(BvarMetricEntity entity);
    SystemBvarMetrics* system_metrics() { return system_metrics_.get(); }

    std::string to_prometheus() const;

private:
    DorisBvarMetrics();

private:
    static const std::string s_registry_name_;

    std::unique_ptr<SystemBvarMetrics> system_metrics_;

    std::unordered_map<std::string, std::vector<std::shared_ptr<BvarMetricEntity>>> entities_map_;
};
} // namespace doris