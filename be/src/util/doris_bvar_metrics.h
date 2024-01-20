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
#include <set>
#include <string>
#include <vector>

#include "util/bvar_metrics.h"
#include "util/system_bvar_metrics.h"
namespace doris {

class DorisBvarMetrics {
public:
    static DorisBvarMetrics* instance() {
        static DorisBvarMetrics metrics;
        return &metrics;
    }

    void initialize(
        bool init_system_metrics = false,
        const std::set<std::string>& disk_devices = std::set<std::string>(),
        const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    BvarMetricRegistry* get_bvar_metric_registry() { return &bvar_metric_registry_; }
    SystemBvarMetrics* get_system_bvar_metrics() { return system_metrics_.get(); }
    BvarMetricEntity* get_server_entity() { return server_metric_entity_.get(); }

    std::string to_prometheus() const;

private:
    DorisBvarMetrics();

private:
    static const std::string s_registry_name_;
    static const std::string s_hook_name_;

    BvarMetricRegistry bvar_metric_registry_;
    
    std::unique_ptr<SystemBvarMetrics> system_metrics_;

    std::shared_ptr<BvarMetricEntity> server_metric_entity_;
};

extern BvarAdderMetric<int64_t> g_adder_timeout_canceled_fragment_count;
extern BvarAdderMetric<int64_t> g_adder_file_created_total;
extern BvarAdderMetric<int64_t> g_adder_fragment_requests_total;
extern BvarAdderMetric<int64_t> g_adder_fragment_request_duration_us;
extern BvarAdderMetric<int64_t> g_adder_query_scan_bytes;
extern BvarAdderMetric<int64_t> g_adder_segment_read_total;

} // namespace doris