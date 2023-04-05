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

#include "util/bvar_metrics.h"

namespace doris {

class DorisBvarMetrics {
public:
    static DorisBvarMetrics* instance() {
        static DorisBvarMetrics metrics;
        return &metrics;
    }
    void initialize();
    void put(BvarMetricEntity entity);
    std::string to_prometheus();

private:
    DorisBvarMetrics() = default;
    std::string name_ = "doris_be";
    std::vector<std::shared_ptr<BvarMetricEntity>> vec_;
    bthread::Mutex mutex_;
};

extern BvarAdderMetric<int64_t> g_adder_timeout_canceled_fragment_count;
extern BvarAdderMetric<int64_t> g_adder_file_created_total;
extern BvarAdderMetric<int64_t> g_adder_fragment_requests_total;
extern BvarAdderMetric<int64_t> g_adder_fragment_request_duration_us;
extern BvarAdderMetric<int64_t> g_adder_query_scan_bytes;
extern BvarAdderMetric<int64_t> g_adder_segment_read_total;

} // namespace doris
