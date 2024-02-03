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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "util/bvar_metrics.h"

namespace doris {

struct CpuBvarMetrics;
struct MemoryBvarMetrics;
struct DiskBvarMetrics;
struct NetworkBvarMetrics;
struct FileDescriptorMetrics;
struct SnmpMetrics;
struct LoadAverageMetrics;
struct ProcMetrics;

class SystemBvarMetrics {
public:
    SystemBvarMetrics(const std::set<std::string>& disk_devices,
                      const std::vector<std::string>& network_interfaces);

    ~SystemBvarMetrics();

    std::string to_prometheus(const std::string& registry_name) const;

    // update metrics
    void update();

private:
    void install_cpu_metrics();
    // On Intel(R) Xeon(R) CPU E5-2450 0 @ 2.10GHz;
    // read /proc/stat would cost about 170us
    void update_cpu_metrics();

    void install_memory_metrics();
    void update_memory_metrics();

    void install_disk_metrics(const std::set<std::string>& disk_devices);
    void update_disk_metrics();

    void get_metrics_from_proc_vmstat();
    void get_cpu_name();

    void install_net_metrics(const std::vector<std::string>& interfaces);
    void update_net_metrics();

private:
    std::map<std::string, CpuBvarMetrics*> cpu_metrics_;
    std::shared_ptr<MemoryBvarMetrics> memory_metrics_;
    std::map<std::string, DiskBvarMetrics*> disk_metrics_;
    std::map<std::string, NetworkBvarMetrics*> network_metrics_;

    std::vector<std::string> cpu_names_;
    int proc_net_dev_version_ = 0;
    char* line_ptr_ = nullptr;
    size_t line_buf_size_ = 0;
    std::unordered_map<std::string, std::vector<std::shared_ptr<BvarMetricEntity>>> entities_map_;
};
} // namespace doris