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

#include "util/palo_metrics.h"

#include "util/debug_util.h"
#include "util/system_metrics.h"

namespace palo {

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
IntCounter PaloMetrics::drop_tablet_requests_total;
IntCounter PaloMetrics::report_all_tablets_requests_total;
IntCounter PaloMetrics::report_tablet_requests_total;
IntCounter PaloMetrics::schema_change_requests_total;
IntCounter PaloMetrics::create_rollup_requests_total;
IntCounter PaloMetrics::storage_migrate_requests_total;
IntCounter PaloMetrics::delete_requests_total;
IntCounter PaloMetrics::cancel_delete_requests_total;
IntCounter PaloMetrics::base_compaction_deltas_total;
IntCounter PaloMetrics::base_compaction_bytes_total;
IntCounter PaloMetrics::cumulative_compaction_deltas_total;
IntCounter PaloMetrics::cumulative_compaction_bytes_total;

// gauges
IntGauge PaloMetrics::memory_pool_bytes_total;

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

#define REGISTER_ENGINE_REQUEST_METRIC(type, metric) \
    _metrics->register_metric( \
        "engine_requests_total", MetricLabels().add("type", #type), &metric)

    REGISTER_ENGINE_REQUEST_METRIC(create_tablet, create_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(drop_tablet, drop_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_all_tablets, report_all_tablets_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_tablet, report_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(schema_change, schema_change_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(create_rollup, create_rollup_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(storage_migrate, storage_migrate_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(delete, delete_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(cancel_delete, cancel_delete_requests_total);

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

    if (init_system_metrics) {
        _system_metrics = new SystemMetrics();
        _system_metrics->install(_metrics, disk_devices, network_interfaces);
    }
}

}
