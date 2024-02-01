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

#include "util/bvar_metrics.h"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>

namespace doris {

struct CpuBvarMetrics;
struct MemoryMetrics;
struct DiskMetrics;
struct NetworkMetrics;
struct FileDescriptorMetrics;
struct SnmpMetrics;
struct LoadAverageMetrics;
struct ProcMetrics;

class SystemBvarMetrics {
public:
    SystemBvarMetrics();
    
    ~SystemBvarMetrics();

    std::string to_prometheus(const std::string& registry_name) const;

    // update metrics
    void update();

private:
    void install_cpu_metrics();
    // On Intel(R) Xeon(R) CPU E5-2450 0 @ 2.10GHz;
    // read /proc/stat would cost about 170us
    void update_cpu_metrics();
    void get_cpu_name();

private:
    std::vector<std::string> cpu_names_;
    std::map<std::string, CpuBvarMetrics*> cpu_metrics_;
    char* line_ptr_ = nullptr;
    size_t line_buf_size_ = 0;
    
    std::unordered_map<std::string, 
                       std::vector<std::shared_ptr<BvarMetricEntity>>> entities_map_;

};
} // namespace doris