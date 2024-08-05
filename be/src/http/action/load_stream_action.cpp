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

#include "http/action/load_stream_action.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

#include "cloud/config.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/olap_common.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/exec_env.h"
#include "runtime/load_stream_mgr.h"
#include "service/backend_options.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

void LoadStreamAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, _get_load_streams().ToString());
}

EasyJson LoadStreamAction::_get_load_streams() {
    EasyJson response;

    auto load_streams = ExecEnv::GetInstance()->load_stream_mgr()->get_all_load_stream_ids();

    response["msg"] = "OK";
    response["code"] = 0;
    EasyJson data = response.Set("data", EasyJson::kObject);
    data["host"] = BackendOptions::get_localhost();
    EasyJson tablets = data.Set("load_streams", EasyJson::kArray);
    for (auto& load_id : load_streams) {
        EasyJson tablet = tablets.PushBack(EasyJson::kObject);
        tablet["load_id"] = load_id;
    }
    response["count"] = load_streams.size();
    return response;
}

} // namespace doris
