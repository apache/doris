// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

#include "util/palo_metrics.h"

#include "util/debug_util.h"
#include "util/file_utils.h"
#include "util/system_metrics.h"

namespace palo {

const char* PaloMetrics::_s_hook_name = "palo_metrics";

PaloMetrics PaloMetrics::_s_palo_metrics;

// counters
IntCounter PaloMetrics::fragment_requests_total;
IntCounter PaloMetrics::fragment_request_duration_us;
IntCounter PaloMetrics::http_requests_total;
IntCounter PaloMetrics::http_request_duration_us;
IntCounter PaloMetrics::http_request_send_bytes;
IntCounter PaloMetrics::query_scan_bytes;
IntCounter PaloMetrics::query_scan_rows;
IntCounter PaloMetrics::ranges_processed_total;
IntCounter PaloMetrics::push_requests_success_total;
IntCounter PaloMetrics::push_requests_fail_total;
IntCounter PaloMetrics::push_request_duration_us;
IntCounter PaloMetrics::push_request_write_bytes;
IntCounter PaloMetrics::push_request_write_rows;
IntCounter PaloMetrics::create_tablet_requests_total;
IntCounter PaloMetrics::create_tablet_requests_failed;
IntCounter PaloMetrics::drop_tablet_requests_total;

IntCounter PaloMetrics::report_all_tablets_requests_total;
IntCounter PaloMetrics::report_all_tablets_requests_failed;
IntCounter PaloMetrics::report_tablet_requests_total;
IntCounter PaloMetrics::report_tablet_requests_failed;
IntCounter PaloMetrics::report_disk_requests_total;
IntCounter PaloMetrics::report_disk_requests_failed;
IntCounter PaloMetrics::report_task_requests_total;
IntCounter PaloMetrics::report_task_requests_failed;

IntCounter PaloMetrics::schema_change_requests_total;
IntCounter PaloMetrics::schema_change_requests_failed;
IntCounter PaloMetrics::create_rollup_requests_total;
IntCounter PaloMetrics::create_rollup_requests_failed;
IntCounter PaloMetrics::storage_migrate_requests_total;
IntCounter PaloMetrics::delete_requests_total;
IntCounter PaloMetrics::delete_requests_failed;
IntCounter PaloMetrics::cancel_delete_requests_total;
IntCounter PaloMetrics::clone_requests_total;
IntCounter PaloMetrics::clone_requests_failed;

IntCounter PaloMetrics::finish_task_requests_total;
IntCounter PaloMetrics::finish_task_requests_failed;

IntCounter PaloMetrics::base_compaction_deltas_total;
IntCounter PaloMetrics::base_compaction_bytes_total;
IntCounter PaloMetrics::base_compaction_request_total;
IntCounter PaloMetrics::base_compaction_request_failed;
IntCounter PaloMetrics::cumulative_compaction_deltas_total;
IntCounter PaloMetrics::cumulative_compaction_bytes_total;
IntCounter PaloMetrics::cumulative_compaction_request_total;
IntCounter PaloMetrics::cumulative_compaction_request_failed;

// gauges
IntGauge PaloMetrics::memory_pool_bytes_total;
IntGauge PaloMetrics::process_thread_num;
IntGauge PaloMetrics::process_fd_num_used;
IntGauge PaloMetrics::process_fd_num_limit_soft;
IntGauge PaloMetrics::process_fd_num_limit_hard;

PaloMetrics::PaloMetrics() : _metrics(nullptr), _system_metrics(nullptr) {
}

PaloMetrics::~PaloMetrics() {
    delete _system_metrics;
    delete _metrics;
}

void PaloMetrics::initialize(const std::string& name,
                             bool init_system_metrics,
                             const std::set<std::string>& disk_devices,
                             const std::vector<std::string>& network_interfaces) {
    _metrics = new MetricRegistry(name);
#define REGISTER_PALO_METRIC(name) _metrics->register_metric(#name, &name)

    // You can put PaloMetrics's metrics initial code here
    REGISTER_PALO_METRIC(fragment_requests_total);
    REGISTER_PALO_METRIC(fragment_request_duration_us);
    REGISTER_PALO_METRIC(http_requests_total);
    REGISTER_PALO_METRIC(http_request_duration_us);
    REGISTER_PALO_METRIC(http_request_send_bytes);
    REGISTER_PALO_METRIC(query_scan_bytes);
    REGISTER_PALO_METRIC(query_scan_rows);
    REGISTER_PALO_METRIC(ranges_processed_total);

    // push request
    _metrics->register_metric(
        "push_requests_total", MetricLabels().add("status", "SUCCESS"),
        &push_requests_success_total);
    _metrics->register_metric(
        "push_requests_total", MetricLabels().add("status", "FAIL"),
        &push_requests_fail_total);
    REGISTER_PALO_METRIC(push_request_duration_us);
    REGISTER_PALO_METRIC(push_request_write_bytes);
    REGISTER_PALO_METRIC(push_request_write_rows);

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
    REGISTER_ENGINE_REQUEST_METRIC(cancel_delete, total, cancel_delete_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(clone, total, clone_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(clone, failed, clone_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(finish_task, total, finish_task_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(finish_task, failed, finish_task_requests_failed);

    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, total, base_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, failed, base_compaction_request_failed);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, total, cumulative_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, failed, cumulative_compaction_request_failed);

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

    // Gauge
    REGISTER_PALO_METRIC(memory_pool_bytes_total);
    REGISTER_PALO_METRIC(process_thread_num);
    REGISTER_PALO_METRIC(process_fd_num_used);
    REGISTER_PALO_METRIC(process_fd_num_limit_soft);
    REGISTER_PALO_METRIC(process_fd_num_limit_hard);

    _metrics->register_hook(_s_hook_name, std::bind(&PaloMetrics::update, this));

    if (init_system_metrics) {
        _system_metrics = new SystemMetrics();
        _system_metrics->install(_metrics, disk_devices, network_interfaces);
    }
}

void PaloMetrics::update() {
    _update_process_thread_num();
    _update_process_fd_num();
}

// get num of thread of palo_be process
// from /proc/pid/task
void PaloMetrics::_update_process_thread_num() {
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

// get num of file descriptor of palo_be process
void PaloMetrics::_update_process_fd_num() {
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
