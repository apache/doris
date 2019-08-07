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

#include <sys/types.h>
#include <unistd.h>

#include "util/doris_metrics.h"

#include "util/debug_util.h"
#include "util/file_utils.h"
#include "util/system_metrics.h"

namespace doris {

const char* DorisMetrics::_s_hook_name = "doris_metrics";

DorisMetrics DorisMetrics::_s_doris_metrics;

// counters
IntCounter DorisMetrics::fragment_requests_total;
IntCounter DorisMetrics::fragment_request_duration_us;
IntCounter DorisMetrics::http_requests_total;
IntCounter DorisMetrics::http_request_duration_us;
IntCounter DorisMetrics::http_request_send_bytes;
IntCounter DorisMetrics::query_scan_bytes;
IntCounter DorisMetrics::query_scan_rows;
IntCounter DorisMetrics::ranges_processed_total;
IntCounter DorisMetrics::push_requests_success_total;
IntCounter DorisMetrics::push_requests_fail_total;
IntCounter DorisMetrics::push_request_duration_us;
IntCounter DorisMetrics::push_request_write_bytes;
IntCounter DorisMetrics::push_request_write_rows;
IntCounter DorisMetrics::create_tablet_requests_total;
IntCounter DorisMetrics::create_tablet_requests_failed;
IntCounter DorisMetrics::drop_tablet_requests_total;

IntCounter DorisMetrics::report_all_tablets_requests_total;
IntCounter DorisMetrics::report_all_tablets_requests_failed;
IntCounter DorisMetrics::report_tablet_requests_total;
IntCounter DorisMetrics::report_tablet_requests_failed;
IntCounter DorisMetrics::report_disk_requests_total;
IntCounter DorisMetrics::report_disk_requests_failed;
IntCounter DorisMetrics::report_task_requests_total;
IntCounter DorisMetrics::report_task_requests_failed;

IntCounter DorisMetrics::schema_change_requests_total;
IntCounter DorisMetrics::schema_change_requests_failed;
IntCounter DorisMetrics::create_rollup_requests_total;
IntCounter DorisMetrics::create_rollup_requests_failed;
IntCounter DorisMetrics::storage_migrate_requests_total;
IntCounter DorisMetrics::delete_requests_total;
IntCounter DorisMetrics::delete_requests_failed;
IntCounter DorisMetrics::clone_requests_total;
IntCounter DorisMetrics::clone_requests_failed;

IntCounter DorisMetrics::finish_task_requests_total;
IntCounter DorisMetrics::finish_task_requests_failed;

IntCounter DorisMetrics::base_compaction_deltas_total;
IntCounter DorisMetrics::base_compaction_bytes_total;
IntCounter DorisMetrics::base_compaction_request_total;
IntCounter DorisMetrics::base_compaction_request_failed;
IntCounter DorisMetrics::cumulative_compaction_deltas_total;
IntCounter DorisMetrics::cumulative_compaction_bytes_total;
IntCounter DorisMetrics::cumulative_compaction_request_total;
IntCounter DorisMetrics::cumulative_compaction_request_failed;

IntCounter DorisMetrics::publish_task_request_total;
IntCounter DorisMetrics::publish_task_failed_total;

IntCounter DorisMetrics::meta_write_request_total;
IntCounter DorisMetrics::meta_write_request_duration_us;
IntCounter DorisMetrics::meta_read_request_total;
IntCounter DorisMetrics::meta_read_request_duration_us;

IntCounter DorisMetrics::txn_begin_request_total;
IntCounter DorisMetrics::txn_commit_request_total;
IntCounter DorisMetrics::txn_rollback_request_total;
IntCounter DorisMetrics::txn_exec_plan_total;
IntCounter DorisMetrics::stream_receive_bytes_total;
IntCounter DorisMetrics::stream_load_rows_total;

IntCounter DorisMetrics::memtable_flush_total;
IntCounter DorisMetrics::memtable_flush_duration_us;

// gauges
IntGauge DorisMetrics::memory_pool_bytes_total;
IntGauge DorisMetrics::process_thread_num;
IntGauge DorisMetrics::process_fd_num_used;
IntGauge DorisMetrics::process_fd_num_limit_soft;
IntGauge DorisMetrics::process_fd_num_limit_hard;
IntGaugeMetricsMap DorisMetrics::disks_total_capacity;
IntGaugeMetricsMap DorisMetrics::disks_avail_capacity;
IntGaugeMetricsMap DorisMetrics::disks_data_used_capacity;
IntGaugeMetricsMap DorisMetrics::disks_state;

IntGauge DorisMetrics::push_request_write_bytes_per_second;
IntGauge DorisMetrics::query_scan_bytes_per_second;
IntGauge DorisMetrics::max_disk_io_util_percent;
IntGauge DorisMetrics::max_network_send_bytes_rate;
IntGauge DorisMetrics::max_network_receive_bytes_rate;

DorisMetrics::DorisMetrics() : _metrics(nullptr), _system_metrics(nullptr) {
}

DorisMetrics::~DorisMetrics() {
    delete _system_metrics;
    delete _metrics;
}

void DorisMetrics::initialize(
        const std::string& name,
        const std::vector<std::string>& paths,
        bool init_system_metrics,
        const std::set<std::string>& disk_devices,
        const std::vector<std::string>& network_interfaces) {
    _metrics = new MetricRegistry(name);
#define REGISTER_DORIS_METRIC(name) _metrics->register_metric(#name, &name)

    // You can put DorisMetrics's metrics initial code here
    REGISTER_DORIS_METRIC(fragment_requests_total);
    REGISTER_DORIS_METRIC(fragment_request_duration_us);
    REGISTER_DORIS_METRIC(http_requests_total);
    REGISTER_DORIS_METRIC(http_request_duration_us);
    REGISTER_DORIS_METRIC(http_request_send_bytes);
    REGISTER_DORIS_METRIC(query_scan_bytes);
    REGISTER_DORIS_METRIC(query_scan_rows);
    REGISTER_DORIS_METRIC(ranges_processed_total);

    REGISTER_DORIS_METRIC(memtable_flush_total);
    REGISTER_DORIS_METRIC(memtable_flush_duration_us);

    // push request
    _metrics->register_metric(
        "push_requests_total", MetricLabels().add("status", "SUCCESS"),
        &push_requests_success_total);
    _metrics->register_metric(
        "push_requests_total", MetricLabels().add("status", "FAIL"),
        &push_requests_fail_total);
    REGISTER_DORIS_METRIC(push_request_duration_us);
    REGISTER_DORIS_METRIC(push_request_write_bytes);
    REGISTER_DORIS_METRIC(push_request_write_rows);

#define REGISTER_ENGINE_REQUEST_METRIC(type, status, metric) \
    _metrics->register_metric( \
        "engine_requests_total", MetricLabels().add("type", #type).add("status", #status), &metric)

    REGISTER_ENGINE_REQUEST_METRIC(create_tablet, total, create_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(create_tablet, failed, create_tablet_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(drop_tablet, total, drop_tablet_requests_total);

    REGISTER_ENGINE_REQUEST_METRIC(report_all_tablets, total, report_all_tablets_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_all_tablets, failed, report_all_tablets_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(report_tablet, total, report_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_tablet, failed, report_tablet_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(report_disk, total, report_disk_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_disk, failed, report_disk_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(report_task, total, report_task_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_task, failed, report_task_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(schema_change, total, schema_change_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(schema_change, failed, schema_change_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(create_rollup, total, create_rollup_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(create_rollup, failed, create_rollup_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(storage_migrate, total, storage_migrate_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(delete, total, delete_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(delete, failed, delete_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(clone, total, clone_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(clone, failed, clone_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(finish_task, total, finish_task_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(finish_task, failed, finish_task_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, total, base_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, failed, base_compaction_request_failed);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, total, cumulative_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, failed, cumulative_compaction_request_failed);

    REGISTER_ENGINE_REQUEST_METRIC(publish, total, publish_task_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(publish, failed, publish_task_failed_total);

    _metrics->register_metric(
        "compaction_deltas_total", MetricLabels().add("type", "base"),
        &base_compaction_deltas_total);
    _metrics->register_metric(
        "compaction_deltas_total", MetricLabels().add("type", "cumulative"),
        &cumulative_compaction_deltas_total);
    _metrics->register_metric(
        "compaction_bytes_total", MetricLabels().add("type", "base"),
        &base_compaction_bytes_total);
    _metrics->register_metric(
        "compaction_bytes_total", MetricLabels().add("type", "cumulative"),
        &cumulative_compaction_bytes_total);

    _metrics->register_metric(
        "meta_request_total", MetricLabels().add("type", "write"),
        &meta_write_request_total);
    _metrics->register_metric(
        "meta_request_total", MetricLabels().add("type", "read"),
        &meta_read_request_total);
    _metrics->register_metric(
        "meta_request_duration", MetricLabels().add("type", "write"),
        &meta_write_request_duration_us);
    _metrics->register_metric(
        "meta_request_duration", MetricLabels().add("type", "read"),
        &meta_read_request_duration_us);

    _metrics->register_metric(
        "txn_request", MetricLabels().add("type", "begin"),
        &txn_begin_request_total);
    _metrics->register_metric(
        "txn_request", MetricLabels().add("type", "commit"),
        &txn_commit_request_total);
    _metrics->register_metric(
        "txn_request", MetricLabels().add("type", "rollback"),
        &txn_rollback_request_total);
    _metrics->register_metric(
        "txn_request", MetricLabels().add("type", "exec"),
        &txn_exec_plan_total);

    _metrics->register_metric(
        "stream_load", MetricLabels().add("type", "receive_bytes"),
        &stream_receive_bytes_total);
    _metrics->register_metric(
        "stream_load", MetricLabels().add("type", "load_rows"),
        &stream_load_rows_total);

    // Gauge
    REGISTER_DORIS_METRIC(memory_pool_bytes_total);
    REGISTER_DORIS_METRIC(process_thread_num);
    REGISTER_DORIS_METRIC(process_fd_num_used);
    REGISTER_DORIS_METRIC(process_fd_num_limit_soft);
    REGISTER_DORIS_METRIC(process_fd_num_limit_hard);

    // disk usage
    for (auto& path : paths) {
        IntGauge* gauge = disks_total_capacity.set_key(path);
        _metrics->register_metric("disks_total_capacity", MetricLabels().add("path", path), gauge);
        gauge = disks_avail_capacity.set_key(path);
        _metrics->register_metric("disks_avail_capacity", MetricLabels().add("path", path), gauge);
        gauge = disks_data_used_capacity.set_key(path);
        _metrics->register_metric("disks_data_used_capacity", MetricLabels().add("path", path), gauge);
        gauge = disks_state.set_key(path);
        _metrics->register_metric("disks_state", MetricLabels().add("path", path), gauge);
    } 

    REGISTER_DORIS_METRIC(push_request_write_bytes_per_second);
    REGISTER_DORIS_METRIC(query_scan_bytes_per_second);
    REGISTER_DORIS_METRIC(max_disk_io_util_percent);
    REGISTER_DORIS_METRIC(max_network_send_bytes_rate);
    REGISTER_DORIS_METRIC(max_network_receive_bytes_rate);

    _metrics->register_hook(_s_hook_name, std::bind(&DorisMetrics::update, this));

    if (init_system_metrics) {
        _system_metrics = new SystemMetrics();
        _system_metrics->install(_metrics, disk_devices, network_interfaces);
    }
}

void DorisMetrics::update() {
    _update_process_thread_num();
    _update_process_fd_num();
}

// get num of thread of doris_be process
// from /proc/pid/task
void DorisMetrics::_update_process_thread_num() {
    int64_t pid = getpid();
    std::stringstream ss;
    ss << "/proc/" << pid << "/task/";

    int64_t count = 0;
    Status st = FileUtils::scan_dir(ss.str(), nullptr, &count);
    if (!st.ok()) {
        LOG(WARNING) << "failed to count thread num from: " << ss.str();
        process_thread_num.set_value(0);
        return;
    }

    process_thread_num.set_value(count);
}

// get num of file descriptor of doris_be process
void DorisMetrics::_update_process_fd_num() {
    int64_t pid = getpid();

    // fd used
    std::stringstream ss;
    ss << "/proc/" << pid << "/fd/";
    int64_t count = 0;
    Status st = FileUtils::scan_dir(ss.str(), nullptr, &count);
    if (!st.ok()) {
        LOG(WARNING) << "failed to count fd from: " << ss.str();
        process_fd_num_used.set_value(0);
        return;
    }
    process_fd_num_used.set_value(count);

    // fd limits
    std::stringstream ss2;
    ss2 << "/proc/" << pid << "/limits";
    FILE* fp = fopen(ss2.str().c_str(), "r");
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open " << ss2.str() << " failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    // /proc/pid/limits
    // Max open files            65536                65536                files
    int64_t values[2];
    size_t line_buf_size = 0;
    char* line_ptr = nullptr;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr, "Max open files %" PRId64 " %" PRId64,
                         &values[0], &values[1]);
        if (num == 2) {
            process_fd_num_limit_soft.set_value(values[0]);
            process_fd_num_limit_hard.set_value(values[1]);
            break;
        }
    }

    if (line_ptr != nullptr) {
        free(line_ptr);
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

}
