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

#include "doris_bvar_metrics.h"

namespace doris {

const std::string DorisBvarMetrics::s_registry_name_ = "doris_be";

void DorisBvarMetrics::initialize(bool init_system_metrics, const std::set<std::string>& disk_devices,
                              const std::vector<std::string>& network_interfaces) {
    auto file_create_total_ptr =
            std::make_shared<BvarMetricEntity>("file_create_total", BvarMetricType::COUNTER);
    entities_.push_back(file_create_total_ptr);
    file_create_total_ptr->register_metric("file_create_total", g_adder_file_created_total);

    auto timeout_canceled_fragment_count =
            std::make_shared<BvarMetricEntity>("timeout_canceled", BvarMetricType::GAUGE);
    entities_.push_back(timeout_canceled_fragment_count);
    timeout_canceled_fragment_count->register_metric("timeout_canceled_fragment_count",
                                         g_adder_timeout_canceled_fragment_count);

    auto test_ptr = std::make_shared<BvarMetricEntity>("test", BvarMetricType::COUNTER);
    entities_.push_back(test_ptr);
    test_ptr->register_metric("fragment_request_total", g_adder_fragment_requests_total);
    test_ptr->register_metric("fragment_request_duration", g_adder_fragment_request_duration_us);
    test_ptr->register_metric("query_scan_byte", g_adder_query_scan_bytes);
    test_ptr->register_metric("segment_read_total", g_adder_segment_read_total);
}

void DorisBvarMetrics::register_entity(BvarMetricEntity entity) {

}

std::string DorisBvarMetrics::to_prometheus() const{
    std::stringstream ss;
    for (auto entity : entities_) {
        ss << entity->to_prometheus(s_registry_name_);
    }
    return ss.str();
}

// timeout_canceled_fragment_count_entity
BvarAdderMetric<int64_t> g_adder_timeout_canceled_fragment_count(BvarMetricType::GAUGE,
                                                                 BvarMetricUnit::NOUNIT,
                                                                 "timeout_canceled_fragment_count",
                                                                 "", "", Labels());
BvarAdderMetric<int64_t> g_adder_file_created_total(BvarMetricType::COUNTER,
                                                    BvarMetricUnit::FILESYSTEM,
                                                    "file_created_total", "", "", Labels());
// test_entity
BvarAdderMetric<int64_t> g_adder_fragment_requests_total(BvarMetricType::COUNTER,
                                                         BvarMetricUnit::REQUESTS,
                                                         "fragment_requests_total",
                                                         "Total fragment requests received.", "",
                                                         Labels());
BvarAdderMetric<int64_t> g_adder_fragment_request_duration_us(BvarMetricType::COUNTER,
                                                              BvarMetricUnit::REQUESTS,
                                                              "fragment_request_duration_us", "",
                                                              "", Labels());
BvarAdderMetric<int64_t> g_adder_query_scan_bytes(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                                  "query_scan_bytes", "", "", Labels());
BvarAdderMetric<int64_t> g_adder_segment_read_total(BvarMetricType::COUNTER,
                                                    BvarMetricUnit::OPERATIONS,
                                                    "segment_read_total",
                                                    "(segment_v2) toal number of segments read",
                                                    "segment_read",
                                                    Labels({{"type", "segment_read_total"}}));
} // namespace doris