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

#include "service/http/action/rotate_credential_action.h"

#include <string>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "service/http/action/action_constants.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "service/http/http_token_manager.h"

namespace doris {

namespace {

constexpr const char* kTokenParam = "token";

bool authenticate_http_token(HttpRequest* req) {
    const auto& token = req->param(kTokenParam);
    if (!HttpTokenManager::instance().verify(token)) {
        LOG(WARNING) << "rejected credential rotation request: unauthorized";
        Status st = Status::NotAuthorized("incorrect token");
        req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, st.to_json());
        return false;
    }
    return true;
}

} // namespace

void RotateHttpTokenAction::handle(HttpRequest* req) {
    if (!authenticate_http_token(req)) {
        return;
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    Status st = HttpTokenManager::instance().rotate();
    if (st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
        return;
    }
    if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_json());
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_json());
}

void RotateCloudUniqueIdAction::handle(HttpRequest* req) {
    if (!authenticate_http_token(req)) {
        return;
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::lock_guard<std::mutex> lock(_rotate_mtx);

    std::string new_id;
    bool found = false;
    Status read_st = config::read_effective_string_config("cloud_unique_id", &new_id, &found);
    if (!read_st.ok()) {
        LOG(WARNING) << "failed to read configured cloud_unique_id during rotation";
        Status err = Status::InternalError("Failed to read configured cloud_unique_id");
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, err.to_json());
        return;
    }
    if (!found || new_id.empty()) {
        LOG(WARNING) << "configured cloud_unique_id is empty during rotation";
        Status err = Status::InvalidArgument("Configured cloud_unique_id must not be empty");
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, err.to_json());
        return;
    }

    if (new_id != config::get_cloud_unique_id()) {
        config::set_cloud_unique_id(new_id, /*from_rotate=*/true);
        LOG(INFO) << "rotate_cloud_unique_id succeeded";
    } else {
        LOG(INFO) << "rotate_cloud_unique_id succeeded (idempotent, unchanged)";
    }
    HttpChannel::send_reply(req, HttpStatus::OK, Status::OK().to_json());
}

} // namespace doris
