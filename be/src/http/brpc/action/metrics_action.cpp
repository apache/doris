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

#include "metrics_action.h"

#include <brpc/http_method.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "util/metrics.h"

namespace doris {
MetricsHandler::MetricsHandler(MetricRegistry* metric_registry)
        : BaseHttpHandler("metrics"), _metric_registry(metric_registry) {}

void MetricsHandler::handle_sync(brpc::Controller* cntl) {
    const std::string& type = *get_param(cntl, "type");
    const std::string& with_tablet = *get_param(cntl, "with_tablet");
    std::string str;
    if (type == "core") {
        str = _metric_registry->to_core_string();
    } else if (type == "json") {
        str = _metric_registry->to_json(with_tablet == "true");
    } else {
        str = _metric_registry->to_prometheus(with_tablet == "true");
    }

    on_succ(cntl, str);
}

bool MetricsHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET;
}
} // namespace doris