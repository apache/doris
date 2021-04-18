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

#include "http/action/metrics_action.h"

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "runtime/exec_env.h"
#include "util/metrics.h"

namespace doris {

void MetricsAction::handle(HttpRequest* req) {
    const std::string& type = req->param("type");
    const std::string& with_tablet = req->param("with_tablet");
    std::string str;
    if (type == "core") {
        str = _metric_registry->to_core_string();
    } else if (type == "json") {
        str = _metric_registry->to_json(with_tablet == "true");
    } else {
        str = _metric_registry->to_prometheus(with_tablet == "true");
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    HttpChannel::send_reply(req, str);
}

} // namespace doris
