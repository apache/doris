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

#include <jni.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "util/jvm_metrics.h"
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
    IntCounter* fragment_requests_total = nullptr;
    IntCounter* fragment_request_duration_us = nullptr;
    IntCounter* query_scan_bytes = nullptr;
    IntCounter* query_scan_rows = nullptr;

    IntCounter* push_requests_success_total = nullptr;
    IntCounter* push_requests_fail_total = nullptr;
    IntCounter* push_request_duration_us = nullptr;
    IntCounter* push_request_write_bytes = nullptr;
    IntCounter* push_request_write_rows = nullptr;
    IntCounter* create_tablet_requests_total = nullptr;
    IntCounter* create_tablet_requests_failed = nullptr;
    IntCounter* drop_tablet_requests_total = nullptr;

    IntCounter* report_all_tablets_requests_skip = nullptr;

    IntCounter* schema_change_requests_total = nullptr;
    IntCounter* schema_change_requests_failed = nullptr;
    IntCounter* create_rollup_requests_total = nullptr;
    IntCounter* create_rollup_requests_failed = nullptr;
    IntCounter* storage_migrate_requests_total = nullptr;
    IntCounter* storage_migrate_v2_requests_total = nullptr;
    IntCounter* storage_migrate_v2_requests_failed = nullptr;
    IntCounter* delete_requests_total = nullptr;
    IntCounter* delete_requests_failed = nullptr;
    IntCounter* clone_requests_total = nullptr;
    IntCounter* clone_requests_failed = nullptr;
    IntCounter* alter_inverted_index_requests_total = nullptr;
    IntCounter* alter_inverted_index_requests_failed = nullptr;

    IntCounter* finish_task_requests_total = nullptr;
    IntCounter* finish_task_requests_failed = nullptr;

    IntCounter* base_compaction_request_total = nullptr;
    IntCounter* base_compaction_request_failed = nullptr;
    IntCounter* cumulative_compaction_request_total = nullptr;
    IntCounter* cumulative_compaction_request_failed = nullptr;
    IntCounter* single_compaction_request_total = nullptr;
    IntCounter* single_compaction_request_failed = nullptr;
    IntCounter* single_compaction_request_cancelled = nullptr;

    IntCounter* base_compaction_deltas_total = nullptr;
    IntCounter* base_compaction_bytes_total = nullptr;
    IntCounter* cumulative_compaction_deltas_total = nullptr;
    IntCounter* cumulative_compaction_bytes_total = nullptr;
    IntCounter* full_compaction_deltas_total = nullptr;
    IntCounter* full_compaction_bytes_total = nullptr;

    IntCounter* publish_task_request_total = nullptr;
    IntCounter* publish_task_failed_total = nullptr;

    // Counters for segment_v2
    // -----------------------
    // total number of segments read
    IntCounter* segment_read_total = nullptr;
    // total number of rows in queried segments (before index pruning)
    IntCounter* segment_row_total = nullptr;

    IntCounter* stream_load_txn_begin_request_total = nullptr;
    IntCounter* stream_load_txn_commit_request_total = nullptr;
    IntCounter* stream_load_txn_rollback_request_total = nullptr;
    IntCounter* stream_receive_bytes_total = nullptr;
    IntCounter* stream_load_rows_total = nullptr;
    IntCounter* load_rows = nullptr;
    IntCounter* load_bytes = nullptr;

    IntCounter* memtable_flush_total = nullptr;
    IntCounter* memtable_flush_duration_us = nullptr;

    IntGauge* memory_pool_bytes_total = nullptr;
    IntGauge* process_thread_num = nullptr;
    IntGauge* process_fd_num_used = nullptr;
    IntGauge* process_fd_num_limit_soft = nullptr;
    IntGauge* process_fd_num_limit_hard = nullptr;

    // the max compaction score of all tablets.
    // Record base and cumulative scores separately, because
    // we need to get the larger of the two.
    IntGauge* tablet_cumulative_max_compaction_score = nullptr;
    IntGauge* tablet_base_max_compaction_score = nullptr;

    IntGauge* all_rowsets_num = nullptr;
    IntGauge* all_segments_num = nullptr;

    // permits have been used for all compaction tasks
    IntGauge* compaction_used_permits = nullptr;
    // permits required by the compaction task which is waiting for permits
    IntGauge* compaction_waitting_permits = nullptr;

    HistogramMetric* tablet_version_num_distribution = nullptr;

    // The following metrics will be calculated
    // by metric calculator
    IntGauge* query_scan_bytes_per_second = nullptr;

    // Metrics related with file reader/writer
    IntCounter* local_file_reader_total = nullptr;
    IntCounter* s3_file_reader_total = nullptr;
    IntCounter* hdfs_file_reader_total = nullptr;
    IntCounter* broker_file_reader_total = nullptr;
    IntCounter* local_file_writer_total = nullptr;
    IntCounter* s3_file_writer_total = nullptr;
    IntCounter* file_created_total = nullptr;
    IntCounter* s3_file_created_total = nullptr;
    IntCounter* local_bytes_read_total = nullptr;
    IntCounter* s3_bytes_read_total = nullptr;
    IntCounter* local_bytes_written_total = nullptr;
    IntCounter* s3_bytes_written_total = nullptr;
    IntGauge* local_file_open_reading = nullptr;
    IntGauge* s3_file_open_reading = nullptr;
    IntGauge* hdfs_file_open_reading = nullptr;
    IntGauge* broker_file_open_reading = nullptr;
    IntGauge* local_file_open_writing = nullptr;
    IntGauge* s3_file_open_writing = nullptr;

    // Size of some global containers
    UIntGauge* rowset_count_generated_and_in_use = nullptr;
    UIntGauge* unused_rowsets_count = nullptr;
    UIntGauge* broker_count = nullptr;
    UIntGauge* data_stream_receiver_count = nullptr;
    UIntGauge* fragment_endpoint_count = nullptr;
    UIntGauge* active_scan_context_count = nullptr;
    UIntGauge* fragment_instance_count = nullptr;
    UIntGauge* load_channel_count = nullptr;
    UIntGauge* result_buffer_block_count = nullptr;
    UIntGauge* result_block_queue_count = nullptr;
    UIntGauge* routine_load_task_count = nullptr;
    UIntGauge* small_file_cache_count = nullptr;
    UIntGauge* stream_load_pipe_count = nullptr;
    UIntGauge* new_stream_load_pipe_count = nullptr;
    UIntGauge* brpc_endpoint_stub_count = nullptr;
    UIntGauge* brpc_function_endpoint_stub_count = nullptr;
    UIntGauge* tablet_writer_count = nullptr;

    UIntGauge* segcompaction_mem_consumption = nullptr;
    UIntGauge* compaction_mem_consumption = nullptr;
    UIntGauge* load_mem_consumption = nullptr;
    UIntGauge* load_channel_mem_consumption = nullptr;
    UIntGauge* memtable_memory_limiter_mem_consumption = nullptr;
    UIntGauge* query_mem_consumption = nullptr;
    UIntGauge* schema_change_mem_consumption = nullptr;
    UIntGauge* storage_migration_mem_consumption = nullptr;
    UIntGauge* tablet_meta_mem_consumption = nullptr;

    // Cache metrics
    UIntGauge* query_cache_memory_total_byte = nullptr;
    UIntGauge* query_cache_sql_total_count = nullptr;
    UIntGauge* query_cache_partition_total_count = nullptr;

    UIntGauge* scanner_thread_pool_queue_size = nullptr;
    UIntGauge* add_batch_task_queue_size = nullptr;
    UIntGauge* send_batch_thread_pool_thread_num = nullptr;
    UIntGauge* send_batch_thread_pool_queue_size = nullptr;
    UIntGauge* fragment_thread_pool_queue_size = nullptr;

    // Upload metrics
    UIntGauge* upload_total_byte = nullptr;
    IntCounter* upload_rowset_count = nullptr;
    IntCounter* upload_fail_count = nullptr;

    UIntGauge* light_work_pool_queue_size = nullptr;
    UIntGauge* heavy_work_pool_queue_size = nullptr;
    UIntGauge* heavy_work_active_threads = nullptr;
    UIntGauge* light_work_active_threads = nullptr;

    UIntGauge* heavy_work_pool_max_queue_size = nullptr;
    UIntGauge* light_work_pool_max_queue_size = nullptr;
    UIntGauge* heavy_work_max_threads = nullptr;
    UIntGauge* light_work_max_threads = nullptr;

    UIntGauge* flush_thread_pool_queue_size = nullptr;
    UIntGauge* flush_thread_pool_thread_num = nullptr;

    UIntGauge* local_scan_thread_pool_queue_size = nullptr;
    UIntGauge* local_scan_thread_pool_thread_num = nullptr;
    UIntGauge* remote_scan_thread_pool_queue_size = nullptr;
    UIntGauge* remote_scan_thread_pool_thread_num = nullptr;
    UIntGauge* limited_scan_thread_pool_queue_size = nullptr;
    UIntGauge* limited_scan_thread_pool_thread_num = nullptr;
    UIntGauge* group_local_scan_thread_pool_queue_size = nullptr;
    UIntGauge* group_local_scan_thread_pool_thread_num = nullptr;

    IntAtomicCounter* num_io_bytes_read_total = nullptr;
    IntAtomicCounter* num_io_bytes_read_from_cache = nullptr;
    IntAtomicCounter* num_io_bytes_read_from_remote = nullptr;

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
    JvmMetrics* jvm_metrics() { return _jvm_metrics.get(); }
    void init_jvm_metrics(JNIEnv* env);

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
    std::unique_ptr<JvmMetrics> _jvm_metrics;

    std::shared_ptr<MetricEntity> _server_metric_entity;
};

}; // namespace doris
