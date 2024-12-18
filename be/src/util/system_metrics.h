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

#include <stddef.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "util/metrics.h"

namespace doris {

struct CpuMetrics;
struct CpuNumberMetrics;
struct MemoryMetrics;
struct DiskMetrics;
struct NetworkMetrics;
struct FileDescriptorMetrics;
struct SnmpMetrics;
struct LoadAverageMetrics;
struct ProcMetrics;

class SystemMetrics {
public:
    SystemMetrics(MetricRegistry* registry, const std::set<std::string>& disk_devices,
                  const std::vector<std::string>& network_interfaces);
    ~SystemMetrics();

    // update metrics
    void update();

    void get_disks_io_time(std::map<std::string, int64_t>* map);
    int64_t get_max_io_util(const std::map<std::string, int64_t>& lst_value, int64_t interval_sec);

    void get_network_traffic(std::map<std::string, int64_t>* send_map,
                             std::map<std::string, int64_t>* rcv_map);
    void get_max_net_traffic(const std::map<std::string, int64_t>& lst_send_map,
                             const std::map<std::string, int64_t>& lst_rcv_map,
                             int64_t interval_sec, int64_t* send_rate, int64_t* rcv_rate);

    double get_load_average_1_min();

    void update_max_disk_io_util_percent(const std::map<std::string, int64_t>& lst_value,
                                         int64_t interval_sec);
    void update_max_network_send_bytes_rate(int64_t max_send_bytes_rate);
    void update_max_network_receive_bytes_rate(int64_t max_receive_bytes_rate);
    void update_allocator_metrics();

    void update_be_avail_cpu_num();

private:
    void _install_cpu_metrics();
    // On Intel(R) Xeon(R) CPU E5-2450 0 @ 2.10GHz;
    // read /proc/stat would cost about 170us
    void _update_cpu_metrics();

    void _install_memory_metrics(MetricEntity* entity);
    void _update_memory_metrics();

    void _install_disk_metrics(const std::set<std::string>& disk_devices);
    void _update_disk_metrics();

    void _install_net_metrics(const std::vector<std::string>& interfaces);
    void _update_net_metrics();

    void _install_fd_metrics(MetricEntity* entity);
    void _update_fd_metrics();

    void _install_snmp_metrics(MetricEntity* entity);
    void _update_snmp_metrics();

    void _install_load_avg_metrics(MetricEntity* entity);
    void _update_load_avg_metrics();

    void _install_proc_metrics(MetricEntity* entity);
    void _update_proc_metrics();

    void get_metrics_from_proc_vmstat();
    void get_cpu_name();

private:
    static const char* _s_hook_name;

    std::map<std::string, CpuMetrics*> _cpu_metrics;
    std::unique_ptr<CpuNumberMetrics> _cpu_num_metrics;
    std::unique_ptr<MemoryMetrics> _memory_metrics;
    std::map<std::string, DiskMetrics*> _disk_metrics;
    std::map<std::string, NetworkMetrics*> _network_metrics;
    std::unique_ptr<FileDescriptorMetrics> _fd_metrics;
    std::unique_ptr<LoadAverageMetrics> _load_average_metrics;
    int _proc_net_dev_version = 0;
    std::unique_ptr<SnmpMetrics> _snmp_metrics;
    std::unique_ptr<ProcMetrics> _proc_metrics;

    std::vector<std::string> _cpu_names;
    char* _line_ptr = nullptr;
    size_t _line_buf_size = 0;
    MetricRegistry* _registry = nullptr;
    std::shared_ptr<MetricEntity> _server_entity;

    IntGauge* max_disk_io_util_percent = nullptr;
    IntGauge* max_network_send_bytes_rate = nullptr;
    IntGauge* max_network_receive_bytes_rate = nullptr;
};

} // namespace doris
