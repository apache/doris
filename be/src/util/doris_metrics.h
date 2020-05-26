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

    IntGauge* set_key(const std::string& key, const MetricUnit unit) {
        metrics.emplace(key, IntGauge(unit));
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
	METRIC_DEFINE_INT_COUNTER(fragment_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(fragment_request_duration_us, MetricUnit::MICROSECONDS);
	METRIC_DEFINE_INT_COUNTER(http_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(http_request_duration_us, MetricUnit::MICROSECONDS);
	METRIC_DEFINE_INT_COUNTER(http_request_send_bytes, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(query_scan_bytes, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(query_scan_rows, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(ranges_processed_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(push_requests_success_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(push_requests_fail_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(push_request_duration_us, MetricUnit::MICROSECONDS);
	METRIC_DEFINE_INT_COUNTER(push_request_write_bytes, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(push_request_write_rows, MetricUnit::ROWS);
	METRIC_DEFINE_INT_COUNTER(create_tablet_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(create_tablet_requests_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(drop_tablet_requests_total, MetricUnit::NUMBER);
	
	METRIC_DEFINE_INT_COUNTER(report_all_tablets_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(report_all_tablets_requests_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(report_tablet_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(report_tablet_requests_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(report_disk_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(report_disk_requests_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(report_task_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(report_task_requests_failed, MetricUnit::NUMBER);
	
	METRIC_DEFINE_INT_COUNTER(schema_change_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(schema_change_requests_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(create_rollup_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(create_rollup_requests_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(storage_migrate_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(delete_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(delete_requests_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(clone_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(clone_requests_failed, MetricUnit::NUMBER);
	
	METRIC_DEFINE_INT_COUNTER(finish_task_requests_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(finish_task_requests_failed, MetricUnit::NUMBER);
	
	METRIC_DEFINE_INT_COUNTER(base_compaction_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(base_compaction_request_failed, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(cumulative_compaction_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(cumulative_compaction_request_failed, MetricUnit::NUMBER);
	
	METRIC_DEFINE_INT_COUNTER(base_compaction_deltas_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(base_compaction_bytes_total, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(cumulative_compaction_deltas_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(cumulative_compaction_bytes_total, MetricUnit::BYTES);
	
	METRIC_DEFINE_INT_COUNTER(publish_task_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(publish_task_failed_total, MetricUnit::NUMBER);
	
	METRIC_DEFINE_INT_COUNTER(meta_write_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(meta_write_request_duration_us, MetricUnit::MICROSECONDS);
	METRIC_DEFINE_INT_COUNTER(meta_read_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(meta_read_request_duration_us, MetricUnit::MICROSECONDS);
	
	// Counters for segment_v2
	// -----------------------
	// total number of segments read
	METRIC_DEFINE_INT_COUNTER(segment_read_total, MetricUnit::NUMBER);
	// total number of rows in queried segments (before index pruning)
	METRIC_DEFINE_INT_COUNTER(segment_row_total, MetricUnit::ROWS);
	// total number of rows selected by short key index
	METRIC_DEFINE_INT_COUNTER(segment_rows_by_short_key, MetricUnit::ROWS);
	// total number of rows selected by zone map index
	METRIC_DEFINE_INT_COUNTER(segment_rows_read_by_zone_map, MetricUnit::ROWS);
	
	METRIC_DEFINE_INT_COUNTER(txn_begin_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(txn_commit_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(txn_rollback_request_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(txn_exec_plan_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(stream_receive_bytes_total, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(stream_load_rows_total, MetricUnit::ROWS);
	METRIC_DEFINE_INT_COUNTER(load_rows_total, MetricUnit::ROWS);
	METRIC_DEFINE_INT_COUNTER(load_bytes_total, MetricUnit::BYTES);
	
	METRIC_DEFINE_INT_COUNTER(memtable_flush_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(memtable_flush_duration_us, MetricUnit::MICROSECONDS);
	
	// Gauges
	METRIC_DEFINE_INT_GAUGE(memory_pool_bytes_total, MetricUnit::BYTES);
	METRIC_DEFINE_INT_GAUGE(process_thread_num, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(process_fd_num_used, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(process_fd_num_limit_soft, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(process_fd_num_limit_hard, MetricUnit::NUMBER);
    IntGaugeMetricsMap disks_total_capacity;
    IntGaugeMetricsMap disks_avail_capacity;
    IntGaugeMetricsMap disks_data_used_capacity;
    IntGaugeMetricsMap disks_state;
	
	// the max compaction score of all tablets.
	// Record base and cumulative scores separately, because
	// we need to get the larger of the two.
	METRIC_DEFINE_INT_GAUGE(tablet_cumulative_max_compaction_score, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(tablet_base_max_compaction_score, MetricUnit::NUMBER);
	
	// The following metrics will be calculated
	// by metric calculator
	METRIC_DEFINE_INT_GAUGE(push_request_write_bytes_per_second, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(query_scan_bytes_per_second, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(max_disk_io_util_percent, MetricUnit::PERCENT);
	METRIC_DEFINE_INT_GAUGE(max_network_send_bytes_rate, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(max_network_receive_bytes_rate, MetricUnit::NUMBER);
	
	// Metrics related with BlockManager
	METRIC_DEFINE_INT_COUNTER(readable_blocks_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(writable_blocks_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(blocks_created_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(blocks_deleted_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_COUNTER(bytes_read_total, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(bytes_written_total, MetricUnit::BYTES);
	METRIC_DEFINE_INT_COUNTER(disk_sync_total, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(blocks_open_reading, MetricUnit::NUMBER);
	METRIC_DEFINE_INT_GAUGE(blocks_open_writing, MetricUnit::NUMBER);
	
	METRIC_DEFINE_INT_COUNTER(blocks_push_remote_duration_us, MetricUnit::MICROSECONDS);
	
	// Size of some global containers
	METRIC_DEFINE_UINT_GAUGE(rowset_count_generated_and_in_use, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(unused_rowsets_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(broker_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(data_stream_receiver_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(fragment_endpoint_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(active_scan_context_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(plan_fragment_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(load_channel_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(result_buffer_block_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(result_block_queue_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(routine_load_task_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(small_file_cache_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(stream_load_pipe_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(brpc_endpoint_stub_count, MetricUnit::NUMBER);
	METRIC_DEFINE_UINT_GAUGE(tablet_writer_count, MetricUnit::NUMBER);

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
