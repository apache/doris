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

#include "util/metrics.h"
#include "util/system_metrics.h"

namespace doris {

#define REGISTER_ENTITY_HOOK_METRIC(entity, owner, metric, func)                        \
    owner->metric = (UIntGauge*)(entity->register_metric<UIntGauge>(&METRIC_##metric)); \
    entity->register_hook(#metric, [&]() { owner->metric->set_value(func()); });

#define REGISTER_HOOK_METRIC(metric, func)                                 \
    REGISTER_ENTITY_HOOK_METRIC(DorisMetrics::instance()->server_entity(), \
                                DorisMetrics::instance(), metric, func)

#define DEREGISTER_ENTITY_HOOK_METRIC(entity, name) \
    entity->deregister_metric(&METRIC_##name);      \
    entity->deregister_hook(#name);

#define DEREGISTER_HOOK_METRIC(name) \
    DEREGISTER_ENTITY_HOOK_METRIC(DorisMetrics::instance()->server_entity(), name)

class DorisMetrics {
public:
    IntCounter* fragment_requests_total;
    IntCounter* fragment_request_duration_us;
    IntCounter* query_scan_bytes;
    IntCounter* query_scan_rows;

    IntCounter* push_requests_success_total;
    IntCounter* push_requests_fail_total;
    IntCounter* push_request_duration_us;
    IntCounter* push_request_write_bytes;
    IntCounter* push_request_write_rows;
    IntCounter* create_tablet_requests_total;
    IntCounter* create_tablet_requests_failed;
    IntCounter* drop_tablet_requests_total;

    IntCounter* report_all_tablets_requests_total;
    IntCounter* report_all_tablets_requests_failed;
    IntCounter* report_tablet_requests_total;
    IntCounter* report_tablet_requests_failed;
    IntCounter* report_all_tablets_requests_skip;
    IntCounter* report_disk_requests_total;
    IntCounter* report_disk_requests_failed;
    IntCounter* report_task_requests_total;
    IntCounter* report_task_requests_failed;

    IntCounter* schema_change_requests_total;
    IntCounter* schema_change_requests_failed;
    IntCounter* create_rollup_requests_total;
    IntCounter* create_rollup_requests_failed;
    IntCounter* storage_migrate_requests_total;
    IntCounter* storage_migrate_v2_requests_total;
    IntCounter* storage_migrate_v2_requests_failed;
    IntCounter* delete_requests_total;
    IntCounter* delete_requests_failed;
    IntCounter* clone_requests_total;
    IntCounter* clone_requests_failed;
    IntCounter* alter_inverted_index_requests_total;
    IntCounter* alter_inverted_index_requests_failed;

    IntCounter* finish_task_requests_total;
    IntCounter* finish_task_requests_failed;

    IntCounter* base_compaction_request_total;
    IntCounter* base_compaction_request_failed;
    IntCounter* cumulative_compaction_request_total;
    IntCounter* cumulative_compaction_request_failed;

    IntCounter* base_compaction_deltas_total;
    IntCounter* base_compaction_bytes_total;
    IntCounter* cumulative_compaction_deltas_total;
    IntCounter* cumulative_compaction_bytes_total;

    IntCounter* publish_task_request_total;
    IntCounter* publish_task_failed_total;

    IntCounter* meta_write_request_total;
    IntCounter* meta_write_request_duration_us;
    IntCounter* meta_read_request_total;
    IntCounter* meta_read_request_duration_us;

    // Counters for segment_v2
    // -----------------------
    // total number of segments read
    IntCounter* segment_read_total;
    // total number of rows in queried segments (before index pruning)
    IntCounter* segment_row_total;

    IntCounter* stream_load_txn_begin_request_total;
    IntCounter* stream_load_txn_commit_request_total;
    IntCounter* stream_load_txn_rollback_request_total;
    IntCounter* stream_receive_bytes_total;
    IntCounter* stream_load_rows_total;
    IntCounter* load_rows;
    IntCounter* load_bytes;

    IntCounter* memtable_flush_total;
    IntCounter* memtable_flush_duration_us;

    IntGauge* memory_pool_bytes_total;
    IntGauge* process_thread_num;
    IntGauge* process_fd_num_used;
    IntGauge* process_fd_num_limit_soft;
    IntGauge* process_fd_num_limit_hard;

    // the max compaction score of all tablets.
    // Record base and cumulative scores separately, because
    // we need to get the larger of the two.
    IntGauge* tablet_cumulative_max_compaction_score;
    IntGauge* tablet_base_max_compaction_score;

    IntGauge* all_rowsets_num;
    IntGauge* all_segments_num;

    // permits have been used for all compaction tasks
    IntGauge* compaction_used_permits;
    // permits required by the compaction task which is waiting for permits
    IntGauge* compaction_waitting_permits;

    HistogramMetric* tablet_version_num_distribution;

    // The following metrics will be calculated
    // by metric calculator
    IntGauge* query_scan_bytes_per_second;

    // Metrics related with file reader/writer
    IntCounter* local_file_reader_total;
    IntCounter* s3_file_reader_total;
    IntCounter* hdfs_file_reader_total;
    IntCounter* broker_file_reader_total;
    IntCounter* local_file_writer_total;
    IntCounter* s3_file_writer_total;
    IntCounter* file_created_total;
    IntCounter* s3_file_created_total;
    IntCounter* local_bytes_read_total;
    IntCounter* s3_bytes_read_total;
    IntCounter* local_bytes_written_total;
    IntCounter* s3_bytes_written_total;
    IntGauge* local_file_open_reading;
    IntGauge* s3_file_open_reading;
    IntGauge* hdfs_file_open_reading;
    IntGauge* broker_file_open_reading;
    IntGauge* local_file_open_writing;
    IntGauge* s3_file_open_writing;

    // Size of some global containers
    UIntGauge* rowset_count_generated_and_in_use;
    UIntGauge* unused_rowsets_count;
    UIntGauge* broker_count;
    UIntGauge* data_stream_receiver_count;
    UIntGauge* fragment_endpoint_count;
    UIntGauge* active_scan_context_count;
    UIntGauge* fragment_instance_count;
    UIntGauge* load_channel_count;
    UIntGauge* result_buffer_block_count;
    UIntGauge* result_block_queue_count;
    UIntGauge* routine_load_task_count;
    UIntGauge* small_file_cache_count;
    UIntGauge* stream_load_pipe_count;
    UIntGauge* new_stream_load_pipe_count;
    UIntGauge* brpc_endpoint_stub_count;
    UIntGauge* brpc_function_endpoint_stub_count;
    UIntGauge* tablet_writer_count;

    UIntGauge* segcompaction_mem_consumption;
    UIntGauge* compaction_mem_consumption;
    UIntGauge* load_mem_consumption;
    UIntGauge* load_channel_mem_consumption;
    UIntGauge* memtable_memory_limiter_mem_consumption;
    UIntGauge* query_mem_consumption;
    UIntGauge* schema_change_mem_consumption;
    UIntGauge* storage_migration_mem_consumption;
    UIntGauge* tablet_meta_mem_consumption;

    // Cache metrics
    UIntGauge* query_cache_memory_total_byte;
    UIntGauge* query_cache_sql_total_count;
    UIntGauge* query_cache_partition_total_count;

    IntCounter* tablet_schema_cache_count;
    UIntGauge* tablet_schema_cache_memory_bytes;
    IntGauge* lru_cache_memory_bytes;

    UIntGauge* scanner_thread_pool_queue_size;
    UIntGauge* add_batch_task_queue_size;
    UIntGauge* send_batch_thread_pool_thread_num;
    UIntGauge* send_batch_thread_pool_queue_size;
    UIntGauge* download_cache_thread_pool_thread_num;
    UIntGauge* download_cache_thread_pool_queue_size;
    UIntGauge* fragment_thread_pool_queue_size;

    // Upload metrics
    UIntGauge* upload_total_byte;
    IntCounter* upload_rowset_count;
    IntCounter* upload_fail_count;

    UIntGauge* light_work_pool_queue_size;
    UIntGauge* heavy_work_pool_queue_size;
    UIntGauge* heavy_work_active_threads;
    UIntGauge* light_work_active_threads;

    UIntGauge* heavy_work_pool_max_queue_size;
    UIntGauge* light_work_pool_max_queue_size;
    UIntGauge* heavy_work_max_threads;
    UIntGauge* light_work_max_threads;

    static DorisMetrics* instance() {
        static DorisMetrics instance;
        return &instance;
    }

    // not thread-safe, call before calling metrics
    void initialize(
            bool init_system_metrics = false,
            const std::set<std::string>& disk_devices = std::set<std::string>(),
            const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    MetricRegistry* metric_registry() { return &_metric_registry; }
    SystemMetrics* system_metrics() { return _system_metrics.get(); }
    MetricEntity* server_entity() { return _server_metric_entity.get(); }

private:
    // Don't allow constructor
    DorisMetrics();

    void _update();
    void _update_process_thread_num();
    void _update_process_fd_num();

private:
    static const std::string _s_registry_name;
    static const std::string _s_hook_name;

    MetricRegistry _metric_registry;

    std::unique_ptr<SystemMetrics> _system_metrics;

    std::shared_ptr<MetricEntity> _server_metric_entity;
};

}; // namespace doris
