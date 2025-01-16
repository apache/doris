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

#include "io/fs/local_file_reader.h"
#include "olap/olap_common.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"

namespace doris {

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(workload_group_cpu_time_sec, doris::MetricUnit::SECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(workload_group_mem_used_bytes, doris::MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(workload_group_remote_scan_bytes, doris::MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(workload_group_total_local_scan_bytes,
                                     doris::MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(workload_group_local_scan_bytes, doris::MetricUnit::BYTES);

#include "common/compile_check_begin.h"

WorkloadGroupMetrics::~WorkloadGroupMetrics() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_entity);
}

WorkloadGroupMetrics::WorkloadGroupMetrics(WorkloadGroup* wg) {
    std::string wg_id_prefix = "workload_group_" + std::to_string(wg->id());
    _entity = DorisMetrics::instance()->metric_registry()->register_entity(
            wg_id_prefix, {{"workload_group", wg->name()}, {"id", std::to_string(wg->id())}});

    INT_COUNTER_METRIC_REGISTER(_entity, workload_group_cpu_time_sec);
    INT_GAUGE_METRIC_REGISTER(_entity, workload_group_mem_used_bytes);
    INT_COUNTER_METRIC_REGISTER(_entity, workload_group_remote_scan_bytes);
    INT_COUNTER_METRIC_REGISTER(_entity, workload_group_total_local_scan_bytes);

    std::vector<DataDirInfo>& data_dir_list = io::BeConfDataDirReader::be_config_data_dir_list;
    for (const auto& data_dir : data_dir_list) {
        std::string data_dir_metric_name = wg_id_prefix + "_io_" + data_dir.metric_name;
        std::shared_ptr<MetricEntity> io_entity =
                DorisMetrics::instance()->metric_registry()->register_entity(
                        data_dir_metric_name, {{"workload_group", wg->name()},
                                               {"path", data_dir.metric_name},
                                               {"id", std::to_string(wg->id())}});
        IntCounter* workload_group_local_scan_bytes = nullptr;
        INT_COUNTER_METRIC_REGISTER(io_entity, workload_group_local_scan_bytes);
        _local_scan_bytes_counter_map.insert({data_dir.path, workload_group_local_scan_bytes});
        _io_entity_list.push_back(io_entity);
    }
}

void WorkloadGroupMetrics::update_cpu_time_nanos(uint64_t delta_cpu_time) {
    _cpu_time_nanos += delta_cpu_time;
}

void WorkloadGroupMetrics::update_memory_used_bytes(int64_t memory_used) {
    _memory_used = memory_used;
}

void WorkloadGroupMetrics::update_local_scan_io_bytes(std::string path, uint64_t delta_io_bytes) {
    workload_group_total_local_scan_bytes->increment(delta_io_bytes);
    auto range = _local_scan_bytes_counter_map.equal_range(path);
    for (auto it = range.first; it != range.second; ++it) {
        it->second->increment((int64_t)delta_io_bytes);
    }
}

void WorkloadGroupMetrics::update_remote_scan_io_bytes(uint64_t delta_io_bytes) {
    workload_group_remote_scan_bytes->increment(delta_io_bytes);
}

void WorkloadGroupMetrics::refresh_metrics() {
    int interval_second = config::workload_group_metrics_interval_ms / 1000;

    // cpu
    uint64_t _current_cpu_time_nanos = _cpu_time_nanos.load();
    uint64_t _cpu_time_sec = _current_cpu_time_nanos / (1000L * 1000L * 1000L);
    workload_group_cpu_time_sec->set_value(_cpu_time_sec);
    _per_sec_cpu_time_nanos = (_current_cpu_time_nanos - _last_cpu_time_nanos) / interval_second;
    _last_cpu_time_nanos = _current_cpu_time_nanos;

    // memory
    workload_group_mem_used_bytes->set_value(_memory_used);

    // local scan
    int64_t current_local_scan_bytes = workload_group_total_local_scan_bytes->value();
    _per_sec_local_scan_bytes =
            (current_local_scan_bytes - _last_local_scan_bytes) / interval_second;
    _last_local_scan_bytes = current_local_scan_bytes;

    // remote scan
    int64_t current_remote_scan_bytes = workload_group_remote_scan_bytes->value();
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
    return workload_group_mem_used_bytes->value();
}

} // namespace doris