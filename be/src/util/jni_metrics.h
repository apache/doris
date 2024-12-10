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

#include "jni.h"
#include "util/metrics.h"

namespace doris {

struct JdbcConnectionMetrics;

class JniMetrics {
public:
    JniMetrics(MetricRegistry* registry);
    ~JniMetrics();
    Status update_jdbc_connection_metrics();

private:
    Status _init();

    static const char* _s_hook_name;
    bool _is_init = false;
    MetricRegistry* _registry = nullptr;
    std::shared_ptr<MetricEntity> _server_entity;

    IntGauge* max_jdbc_scan_connection_percent;
    jclass _jdbc_data_source_clz;
    jmethodID _get_connection_percent_id;
    std::unordered_map<std::string, std::shared_ptr<JdbcConnectionMetrics>>
            _jdbc_connection_metrics;
};

} // namespace doris
