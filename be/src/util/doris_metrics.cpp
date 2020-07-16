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

#include "env/env.h"

#include "util/debug_util.h"
#include "util/file_utils.h"
#include "util/system_metrics.h"

namespace doris {

DorisMetrics::DorisMetrics() : _name("doris_be"), _hook_name("doris_metrics"), _metrics(_name) {
#define REGISTER_DORIS_METRIC(name) _metrics.register_metric(#name, &name)
    // You can put DorisMetrics's metrics initial code here
    REGISTER_DORIS_METRIC(fragment_requests_total);
    REGISTER_DORIS_METRIC(fragment_request_duration_us);
    REGISTER_DORIS_METRIC(http_requests_total);
    REGISTER_DORIS_METRIC(http_request_send_bytes);
    REGISTER_DORIS_METRIC(query_scan_bytes);
    REGISTER_DORIS_METRIC(query_scan_rows);

    REGISTER_DORIS_METRIC(memtable_flush_total);
    REGISTER_DORIS_METRIC(memtable_flush_duration_us);

    // push request
    _metrics.register_metric(
        "push_requests_total", MetricLabels().add("status", "SUCCESS"),
        &push_requests_success_total);
    _metrics.register_metric(
        "push_requests_total", MetricLabels().add("status", "FAIL"),
        &push_requests_fail_total);
    REGISTER_DORIS_METRIC(push_request_duration_us);
    REGISTER_DORIS_METRIC(push_request_write_bytes);
    REGISTER_DORIS_METRIC(push_request_write_rows);

#define REGISTER_ENGINE_REQUEST_METRIC(type, status, metric) \
    _metrics.register_metric( \
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

    _metrics.register_metric(
        "compaction_deltas_total", MetricLabels().add("type", "base"),
        &base_compaction_deltas_total);
    _metrics.register_metric(
        "compaction_deltas_total", MetricLabels().add("type", "cumulative"),
        &cumulative_compaction_deltas_total);
    _metrics.register_metric(
        "compaction_bytes_total", MetricLabels().add("type", "base"),
        &base_compaction_bytes_total);
    _metrics.register_metric(
        "compaction_bytes_total", MetricLabels().add("type", "cumulative"),
        &cumulative_compaction_bytes_total);

    _metrics.register_metric(
        "meta_request_total", MetricLabels().add("type", "write"),
        &meta_write_request_total);
    _metrics.register_metric(
        "meta_request_total", MetricLabels().add("type", "read"),
        &meta_read_request_total);
    _metrics.register_metric(
        "meta_request_duration", MetricLabels().add("type", "write"),
        &meta_write_request_duration_us);
    _metrics.register_metric(
        "meta_request_duration", MetricLabels().add("type", "read"),
        &meta_read_request_duration_us);

    _metrics.register_metric(
        "segment_read", MetricLabels().add("type", "segment_total_read_times"),
        &segment_read_total);
    _metrics.register_metric(
        "segment_read", MetricLabels().add("type", "segment_total_row_num"),
        &segment_row_total);
    _metrics.register_metric(
        "segment_read", MetricLabels().add("type", "segment_rows_by_short_key"),
        &segment_rows_by_short_key);
    _metrics.register_metric(
        "segment_read", MetricLabels().add("type", "segment_rows_read_by_zone_map"),
        &segment_rows_read_by_zone_map);

    _metrics.register_metric(
        "txn_request", MetricLabels().add("type", "begin"),
        &txn_begin_request_total);
    _metrics.register_metric(
        "txn_request", MetricLabels().add("type", "commit"),
        &txn_commit_request_total);
    _metrics.register_metric(
        "txn_request", MetricLabels().add("type", "rollback"),
        &txn_rollback_request_total);
    _metrics.register_metric(
        "txn_request", MetricLabels().add("type", "exec"),
        &txn_exec_plan_total);

    _metrics.register_metric(
        "stream_load", MetricLabels().add("type", "receive_bytes"),
        &stream_receive_bytes_total);
    _metrics.register_metric(
        "stream_load", MetricLabels().add("type", "load_rows"),
        &stream_load_rows_total);
    _metrics.register_metric("load_rows", &load_rows_total);
    _metrics.register_metric("load_bytes", &load_bytes_total);

    // Gauge
    REGISTER_DORIS_METRIC(memory_pool_bytes_total);
    REGISTER_DORIS_METRIC(process_thread_num);
    REGISTER_DORIS_METRIC(process_fd_num_used);
    REGISTER_DORIS_METRIC(process_fd_num_limit_soft);
    REGISTER_DORIS_METRIC(process_fd_num_limit_hard);

    REGISTER_DORIS_METRIC(tablet_cumulative_max_compaction_score);
    REGISTER_DORIS_METRIC(tablet_base_max_compaction_score);

    REGISTER_DORIS_METRIC(push_request_write_bytes_per_second);
    REGISTER_DORIS_METRIC(query_scan_bytes_per_second);
    REGISTER_DORIS_METRIC(max_disk_io_util_percent);
    REGISTER_DORIS_METRIC(max_network_send_bytes_rate);
    REGISTER_DORIS_METRIC(max_network_receive_bytes_rate);

    _metrics.register_hook(_hook_name, std::bind(&DorisMetrics::_update, this));

    REGISTER_DORIS_METRIC(readable_blocks_total);
    REGISTER_DORIS_METRIC(writable_blocks_total);
    REGISTER_DORIS_METRIC(blocks_created_total);
    REGISTER_DORIS_METRIC(blocks_deleted_total);
    REGISTER_DORIS_METRIC(bytes_read_total);
    REGISTER_DORIS_METRIC(bytes_written_total);
    REGISTER_DORIS_METRIC(disk_sync_total);
    REGISTER_DORIS_METRIC(blocks_open_reading);
    REGISTER_DORIS_METRIC(blocks_open_writing);
}

void DorisMetrics::initialize(
        const std::vector<std::string>& paths,
        bool init_system_metrics,
        const std::set<std::string>& disk_devices,
        const std::vector<std::string>& network_interfaces) {
    // disk usage
    for (auto& path : paths) {
        IntGauge* gauge = disks_total_capacity.set_key(path, MetricUnit::BYTES);
        _metrics.register_metric("disks_total_capacity", MetricLabels().add("path", path), gauge);
        gauge = disks_avail_capacity.set_key(path, MetricUnit::BYTES);
        _metrics.register_metric("disks_avail_capacity", MetricLabels().add("path", path), gauge);
        gauge = disks_data_used_capacity.set_key(path, MetricUnit::BYTES);
        _metrics.register_metric("disks_data_used_capacity", MetricLabels().add("path", path), gauge);
        gauge = disks_state.set_key(path, MetricUnit::NOUNIT);
        _metrics.register_metric("disks_state", MetricLabels().add("path", path), gauge);
    }

    if (init_system_metrics) {
        _system_metrics.install(&_metrics, disk_devices, network_interfaces);
    }
}

void DorisMetrics::_update() {
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
    Status st = FileUtils::get_children_count(Env::Default(), ss.str(), &count);
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
    Status st = FileUtils::get_children_count(Env::Default(), ss.str(), &count);
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
