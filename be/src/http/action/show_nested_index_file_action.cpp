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

#include "http/action/show_nested_index_file_action.h"

#include <rapidjson/rapidjson.h>

#include <exception>
#include <string>

#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/stopwatch.hpp"

namespace doris {
using namespace ErrorCode;

const static std::string HEADER_JSON = "application/json";

ShowNestedIndexFileAction::ShowNestedIndexFileAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                                     TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype) {}

// show the nested inverted index file in the tablet
Status ShowNestedIndexFileAction::_handle_show_nested_index_file(HttpRequest* req,
                                                                 std::string* json_meta) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    uint64_t tablet_id = 0;
    try {
        tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id;
        return Status::InternalError("convert failed, {}", e.what());
    }

    auto tablet = DORIS_TRY(ExecEnv::get_tablet(tablet_id));
    RETURN_IF_ERROR(tablet->show_nested_index_file(json_meta));
    return Status::OK();
}

void ShowNestedIndexFileAction::handle(HttpRequest* req) {
    MonotonicStopWatch timer;
    timer.start();

    std::string json_meta;
    Status status = _handle_show_nested_index_file(req, &json_meta);
    std::string status_result = status.to_json();
    timer.stop();
    LOG(INFO) << "handle show_nested_index_file request finished, result:" << status_result
              << ", use time = " << timer.elapsed_time() / 1000000 << "ms";
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, json_meta);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
    }
}

} // end namespace doris
