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

#include "runtime/workload_group/workload_group_metrics.h"

#include "common/config.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"

namespace doris {

WorkloadGroupMetrics::~WorkloadGroupMetrics() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_entity);
}

WorkloadGroupMetrics::WorkloadGroupMetrics(WorkloadGroup* wg) {
    _entity = DorisMetrics::instance()->metric_registry()->register_entity(
            "workload_group." + wg->name(), {{"name", wg->name()}});

    _cpu_time_metric = std::make_unique<doris::MetricPrototype>(
            doris::MetricType::COUNTER, doris::MetricUnit::SECONDS, "workload_group_cpu_time_sec");
    _cpu_time_counter =
            (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(_cpu_time_metric.get()));

    _mem_used_bytes_metric = std::make_unique<doris::MetricPrototype>(
            doris::MetricType::COUNTER, doris::MetricUnit::BYTES, "workload_group_mem_used_bytes");
    _mem_used_bytes_counter = (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(
            _mem_used_bytes_metric.get()));

    _local_scan_bytes_metric = std::make_unique<doris::MetricPrototype>(
            doris::MetricType::COUNTER, doris::MetricUnit::BYTES,
            "workload_group_local_scan_bytes");
    _local_scan_bytes_counter = (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(
            _local_scan_bytes_metric.get()));

    _remote_scan_bytes_metric = std::make_unique<doris::MetricPrototype>(
            doris::MetricType::COUNTER, doris::MetricUnit::BYTES,
            "workload_group_remote_scan_bytes");
    _remote_scan_bytes_counter = (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(
            _remote_scan_bytes_metric.get()));

    for (const auto& [key, io_throttle] : wg->_scan_io_throttle_map) {
        std::unique_ptr<doris::MetricPrototype> metric = std::make_unique<doris::MetricPrototype>(
                doris::MetricType::COUNTER, doris::MetricUnit::BYTES,
                "workload_group_local_scan_bytes_" + io_throttle->metric_name());
        _local_scan_bytes_counter_map[key] =
                (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(metric.get()));
        _local_scan_bytes_metric_map[key] = std::move(metric);
    }
}

void WorkloadGroupMetrics::update_cpu_time_nanos(uint64_t delta_cpu_time) {
    _cpu_time_nanos += delta_cpu_time;
}

void WorkloadGroupMetrics::update_memory_used_bytes(int64_t memory_used) {
    _memory_used = memory_used;
}

void WorkloadGroupMetrics::update_local_scan_io_bytes(std::string path, uint64_t delta_io_bytes) {
    _local_scan_bytes_counter->increment(delta_io_bytes);
    _local_scan_bytes_counter_map[path]->increment((int64_t)delta_io_bytes);
}

void WorkloadGroupMetrics::update_remote_scan_io_bytes(uint64_t delta_io_bytes) {
    _remote_scan_bytes_counter->increment(delta_io_bytes);
}

void WorkloadGroupMetrics::refresh_metrics() {
    int interval_second = config::workload_group_metrics_interval_ms / 1000;

    // cpu
    uint64_t _current_cpu_time_nanos = _cpu_time_nanos.load();
    uint64_t _cpu_time_sec = _current_cpu_time_nanos / (1000L * 1000L * 1000L);
    _cpu_time_counter->set_value(_cpu_time_sec);
    _per_sec_cpu_time_nanos = (_current_cpu_time_nanos - _last_cpu_time_nanos) / interval_second;
    _last_cpu_time_nanos = _current_cpu_time_nanos;

    // memory
    _mem_used_bytes_counter->set_value(_memory_used);

    // local scan
    int64_t current_local_scan_bytes = _local_scan_bytes_counter->value();
    _per_sec_local_scan_bytes =
            (current_local_scan_bytes - _last_local_scan_bytes) / interval_second;
    _last_local_scan_bytes = current_local_scan_bytes;

    // remote scan
    int64_t current_remote_scan_bytes = _remote_scan_bytes_counter->value();
    _per_sec_remote_scan_bytes =
            (current_remote_scan_bytes - _last_remote_scan_bytes) / interval_second;
    _last_remote_scan_bytes = current_remote_scan_bytes;
}

uint64_t WorkloadGroupMetrics::get_cpu_time_nanos_per_second() {
    return _per_sec_cpu_time_nanos.load();
}

int64_t WorkloadGroupMetrics::get_local_scan_bytes_per_second() {
    return _per_sec_local_scan_bytes.load();
}

int64_t WorkloadGroupMetrics::get_remote_scan_bytes_per_second() {
    return _last_remote_scan_bytes.load();
}

int64_t WorkloadGroupMetrics::get_memory_used() {
    return _mem_used_bytes_counter->value();
}

} // namespace doris