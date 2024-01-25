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

#include "http/action/debug_point_action.h"

#include "common/config.h"
#include "http/http_channel.h"
#include "http/http_status.h"
#include "util/debug_points.h"
#include "util/time.h"

namespace doris {

void BaseDebugPointAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();
    Status status;
    if (config::enable_debug_points) {
        status = _handle(req);
    } else {
        status = Status::InternalError(
                "Disable debug points. please check config::enable_debug_points");
    }
    std::string result = status.to_json();
    LOG(INFO) << "handle request result:" << result;
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, result);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, result);
    }
}

Status AddDebugPointAction::_handle(HttpRequest* req) {
    std::string name = req->param("debug_point");
    std::string execute = req->param("execute");
    std::string timeout = req->param("timeout");
    if (name.empty()) {
        return Status::InternalError("Empty debug point name");
    }
    auto debug_point = std::make_shared<DebugPoint>();
    try {
        if (!execute.empty()) {
            debug_point->execute_limit = std::stol(execute);
        }
    } catch (const std::exception& e) {
        return Status::InternalError("Invalid execute limit format, execute {}, err {}", execute,
                                     e.what());
    }
    try {
        if (!timeout.empty()) {
            int64_t timeout_second = std::stol(timeout);
            if (timeout_second > 0) {
                debug_point->expire_ms = MonotonicMillis() + timeout_second * MILLIS_PER_SEC;
            }
        }
    } catch (const std::exception& e) {
        return Status::InternalError("Invalid timeout format, timeout {}, err {}", timeout,
                                     e.what());
    }

    debug_point->params = *(req->params());

    DebugPoints::instance()->add(name, debug_point);

    return Status::OK();
}

Status RemoveDebugPointAction::_handle(HttpRequest* req) {
    std::string debug_point = req->param("debug_point");
    if (debug_point.empty()) {
        return Status::InternalError("Empty debug point name");
    }

    DebugPoints::instance()->remove(debug_point);

    return Status::OK();
}

Status ClearDebugPointsAction::_handle(HttpRequest* req) {
    DebugPoints::instance()->clear();

    return Status::OK();
}

} // namespace doris
