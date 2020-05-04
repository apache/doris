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

#ifndef DORIS_BE_SRC_COMMON_UTIL_DORIS_METRICS_H
#define DORIS_BE_SRC_COMMON_UTIL_DORIS_METRICS_H

#include <set>
#include <string>
#include <vector>
#include <unordered_map>

#include "util/metrics.h"
#include "util/system_metrics.h"

namespace doris {

class IntGaugeMetricsMap {
public:
    void set_metric(const std::string& key, int64_t val) {
        auto metric = metrics.find(key);
        if (metric != metrics.end()) {
            metric->second.set_value(val);
        }
    }

    IntGauge* set_key(const std::string& key) {
        metrics.emplace(key, IntGauge());
        return &metrics.find(key)->second;
    }

private:
    std::unordered_map<std::string, IntGauge> metrics;
};

#define REGISTER_GAUGE_DORIS_METRIC(name, func) \
  DorisMetrics::instance()->metrics()->register_metric(#name, &DorisMetrics::instance()->name); \
  DorisMetrics::instance()->metrics()->register_hook(#name, [&]() { \
      DorisMetrics::instance()->name.set_value(func());  \
});

class DorisMetrics {
public:
    // counters
    IntCounter fragment_requests_total;
    IntCounter fragment_request_duration_us;
    IntCounter http_requests_total;
    IntCounter http_request_duration_us;
    IntCounter http_request_send_bytes;
    IntCounter query_scan_bytes;
    IntCounter query_scan_rows;
    IntCounter ranges_processed_total;
    IntCounter push_requests_success_total;
    IntCounter push_requests_fail_total;
    IntCounter push_request_duration_us;
    IntCounter push_request_write_bytes;
    IntCounter push_request_write_rows;
    IntCounter create_tablet_requests_total;
    IntCounter create_tablet_requests_failed;
    IntCounter drop_tablet_requests_total;

    IntCounter report_all_tablets_requests_total;
    IntCounter report_all_tablets_requests_failed;
    IntCounter report_tablet_requests_total;
    IntCounter report_tablet_requests_failed;
    IntCounter report_disk_requests_total;
    IntCounter report_disk_requests_failed;
    IntCounter report_task_requests_total;
    IntCounter report_task_requests_failed;

    IntCounter schema_change_requests_total;
    IntCounter schema_change_requests_failed;
    IntCounter create_rollup_requests_total;
    IntCounter create_rollup_requests_failed;
    IntCounter storage_migrate_requests_total;
    IntCounter delete_requests_total;
    IntCounter delete_requests_failed;
    IntCounter clone_requests_total;
    IntCounter clone_requests_failed;

    IntCounter finish_task_requests_total;
    IntCounter finish_task_requests_failed;

    IntCounter base_compaction_request_total;
    IntCounter base_compaction_request_failed;
    IntCounter cumulative_compaction_request_total;
    IntCounter cumulative_compaction_request_failed;

    IntCounter base_compaction_deltas_total;
    IntCounter base_compaction_bytes_total;
    IntCounter cumulative_compaction_deltas_total;
    IntCounter cumulative_compaction_bytes_total;

    IntCounter publish_task_request_total;
    IntCounter publish_task_failed_total;

    IntCounter meta_write_request_total;
    IntCounter meta_write_request_duration_us;
    IntCounter meta_read_request_total;
    IntCounter meta_read_request_duration_us;

    // Counters for segment_v2
    // -----------------------
    // total number of segments read
    IntCounter segment_read_total;
    // total number of rows in queried segments (before index pruning)
    IntCounter segment_row_total;
    // total number of rows selected by short key index
    IntCounter segment_rows_by_short_key;
    // total number of rows selected by zone map index
    IntCounter segment_rows_read_by_zone_map;

    IntCounter txn_begin_request_total;
    IntCounter txn_commit_request_total;
    IntCounter txn_rollback_request_total;
    IntCounter txn_exec_plan_total;
    IntCounter stream_receive_bytes_total;
    IntCounter stream_load_rows_total;

    IntCounter memtable_flush_total;
    IntCounter memtable_flush_duration_us;

    // Gauges
    IntGauge memory_pool_bytes_total;
    IntGauge process_thread_num;
    IntGauge process_fd_num_used;
    IntGauge process_fd_num_limit_soft;
    IntGauge process_fd_num_limit_hard;
    IntGaugeMetricsMap disks_total_capacity;
    IntGaugeMetricsMap disks_avail_capacity;
    IntGaugeMetricsMap disks_data_used_capacity;
    IntGaugeMetricsMap disks_state;

    // the max compaction score of all tablets.
    // Record base and cumulative scores separately, because
    // we need to get the larger of the two.
    IntGauge tablet_cumulative_max_compaction_score;
    IntGauge tablet_base_max_compaction_score;

    // The following metrics will be calculated
    // by metric calculator
    IntGauge push_request_write_bytes_per_second;
    IntGauge query_scan_bytes_per_second;
    IntGauge max_disk_io_util_percent;
    IntGauge max_network_send_bytes_rate;
    IntGauge max_network_receive_bytes_rate;

    // Metrics related with BlockManager
    IntCounter readable_blocks_total;
    IntCounter writable_blocks_total;
    IntCounter blocks_created_total;
    IntCounter blocks_deleted_total;
    IntCounter bytes_read_total;
    IntCounter bytes_written_total;
    IntCounter disk_sync_total;
    IntGauge blocks_open_reading;
    IntGauge blocks_open_writing;

    IntCounter blocks_push_remote_duration_us;

    // Size of some global containers
    UIntGauge rowset_count_generated_and_in_use;
    UIntGauge unused_rowsets_count;
    UIntGauge broker_count;
    UIntGauge data_stream_receiver_count;
    UIntGauge fragment_endpoint_count;
    UIntGauge active_scan_context_count;
    UIntGauge plan_fragment_count;
    UIntGauge load_channel_count;
    UIntGauge result_buffer_block_count;
    UIntGauge result_block_queue_count;
    UIntGauge routine_load_task_count;
    UIntGauge small_file_cache_count;
    UIntGauge stream_load_pipe_count;
    UIntGauge brpc_endpoint_stub_count;
    UIntGauge tablet_writer_count;

    static DorisMetrics* instance() {
        static DorisMetrics instance;
        return &instance;
    }

    // not thread-safe, call before calling metrics
    void initialize(
        const std::vector<std::string>& paths = std::vector<std::string>(),
        bool init_system_metrics = false,
        const std::set<std::string>& disk_devices = std::set<std::string>(),
        const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    MetricRegistry* metrics() { return &_metrics; }
    SystemMetrics* system_metrics() { return &_system_metrics; }

private:
    // Don't allow constrctor
    DorisMetrics();

    void _update();
    void _update_process_thread_num();
    void _update_process_fd_num();

private:
    const char* _name;
    const char* _hook_name;

    MetricRegistry _metrics;
    SystemMetrics _system_metrics;
};

};

#endif
