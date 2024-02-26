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

#include "util/bvar_metrics.h"

namespace doris {

struct CpuBvarMetrics;
struct MemoryBvarMetrics;
struct DiskBvarMetrics;
struct NetworkBvarMetrics;
struct FileDescriptorBvarMetrics;
struct SnmpBvarMetrics;
struct LoadAverageBvarMetrics;
struct ProcBvarMetrics;

class SystemBvarMetrics {
public:
    SystemBvarMetrics(BvarMetricRegistry* registry, const std::set<std::string>& disk_devices,
                      const std::vector<std::string>& network_interfaces);

    ~SystemBvarMetrics();

    // update metrics
    void update();

    void get_disks_io_time(std::map<std::string, int64_t>* map);
    int64_t get_max_io_util(const std::map<std::string, int64_t>& lst_value, int64_t interval_sec);

    void get_network_traffic(std::map<std::string, int64_t>* send_map,
                             std::map<std::string, int64_t>* rcv_map);
    void get_max_net_traffic(const std::map<std::string, int64_t>& lst_send_map,
                             const std::map<std::string, int64_t>& lst_rcv_map,
                             int64_t interval_sec, int64_t* send_rate, int64_t* rcv_rate);

    void update_max_disk_io_util_percent(const std::map<std::string, int64_t>& lst_value,
                                         int64_t interval_sec);
    void update_max_network_send_bytes_rate(int64_t max_send_bytes_rate);
    void update_max_network_receive_bytes_rate(int64_t max_receive_bytes_rate);
    void update_allocator_metrics();

    //for UT
    // CpuBvarMetrics* cpu_metrics(const std::string& name) { return cpu_metrics_[name]; }
    // MemoryBvarMetrics* memory_metrics() { return memory_metrics_.get(); }
    // DiskBvarMetrics* disk_metrics(const std::string& name) { return disk_metrics_[name]; }
    // NetworkBvarMetrics* network_metrics(const std::string& name) { return network_metrics_[name]; }
    // FileDescriptorBvarMetrics* fd_metrics() { return fd_metrics_.get(); }
    // SnmpBvarMetrics* snmp_metrics() { return snmp_metrics_.get(); }
    // LoadAverageBvarMetrics* load_average_metrics() { return load_average_metrics_.get(); }
    // ProcBvarMetrics* proc_metrics() { return proc_metrics_.get(); }

private:
    void install_cpu_metrics();
    // On Intel(R) Xeon(R) CPU E5-2450 0 @ 2.10GHz;
    // read /proc/stat would cost about 170us
    void update_cpu_metrics();

    void install_memory_metrics(BvarMetricEntity* entity);
    void update_memory_metrics();

    void install_disk_metrics(const std::set<std::string>& disk_devices);
    void update_disk_metrics();

    void install_net_metrics(const std::vector<std::string>& interfaces);
    void update_net_metrics();

    void install_fd_metrics(BvarMetricEntity* entity);
    void update_fd_metrics();

    void install_snmp_metrics(BvarMetricEntity* entity);
    void update_snmp_metrics();

    void install_load_avg_metrics(BvarMetricEntity* entity);
    void update_load_avg_metrics();

    void install_proc_metrics(BvarMetricEntity* entity);
    void update_proc_metrics();

    void get_metrics_from_proc_vmstat();
    void get_cpu_name();

    void install_max_metrics(BvarMetricEntity* entity);

    static const char* s_hook_name_;

    std::map<std::string, CpuBvarMetrics*> cpu_metrics_;
    std::shared_ptr<MemoryBvarMetrics> memory_metrics_;
    std::map<std::string, DiskBvarMetrics*> disk_metrics_;
    std::map<std::string, NetworkBvarMetrics*> network_metrics_;
    std::shared_ptr<FileDescriptorBvarMetrics> fd_metrics_;
    std::shared_ptr<SnmpBvarMetrics> snmp_metrics_;
    std::shared_ptr<LoadAverageBvarMetrics> load_average_metrics_;
    std::shared_ptr<ProcBvarMetrics> proc_metrics_;
    int proc_net_dev_version_ = 0;

    std::vector<std::string> cpu_names_;
    BvarMetricRegistry* registry_ = nullptr;
    std::shared_ptr<BvarMetricEntity> server_entity_;

    std::shared_ptr<BvarAdderMetric<int64_t>> max_disk_io_util_percent;
    std::shared_ptr<BvarAdderMetric<int64_t>> max_network_send_bytes_rate;
    std::shared_ptr<BvarAdderMetric<int64_t>> max_network_receive_bytes_rate;

    bthread::Mutex mutex_;
};
} // namespace doris