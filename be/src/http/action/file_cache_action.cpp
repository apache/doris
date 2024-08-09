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

#include "file_cache_action.h"

#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "io/cache/block_file_cache_factory.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta.h"
#include "util/easy_json.h"

namespace doris {

constexpr static std::string_view HEADER_JSON = "application/json";
constexpr static std::string_view OP = "op";
constexpr static std::string_view SYNC = "sync";
constexpr static std::string_view PATH = "path";
constexpr static std::string_view CLEAR = "clear";
constexpr static std::string_view RESET = "reset";
constexpr static std::string_view CAPACITY = "capacity";
constexpr static std::string_view RELEASE = "release";
constexpr static std::string_view BASE_PATH = "base_path";
constexpr static std::string_view RELEASED_ELEMENTS = "released_elements";

Status FileCacheAction::_handle_header(HttpRequest* req, std::string* json_metrics) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.data());
    std::string operation = req->param(OP.data());
    Status st = Status::OK();
    if (operation == RELEASE) {
        size_t released = 0;
        const std::string& base_path = req->param(BASE_PATH.data());
        if (!base_path.empty()) {
            released = io::FileCacheFactory::instance()->try_release(base_path);
        } else {
            released = io::FileCacheFactory::instance()->try_release();
        }
        EasyJson json;
        json[RELEASED_ELEMENTS.data()] = released;
        *json_metrics = json.ToString();
    } else if (operation == CLEAR) {
        const std::string& sync = req->param(SYNC.data());
        auto ret = io::FileCacheFactory::instance()->clear_file_caches(to_lower(sync) == "true");
    } else if (operation == RESET) {
        std::string capacity = req->param(CAPACITY.data());
        int64_t new_capacity = 0;
        bool parse = true;
        try {
            new_capacity = std::stoll(capacity);
        } catch (...) {
            parse = false;
        }
        if (!parse || new_capacity <= 0) {
            st = Status::InvalidArgument(
                    "The capacity {} failed to be parsed, the capacity needs to be in "
                    "the interval (0, INT64_MAX]",
                    capacity);
        } else {
            const std::string& path = req->param(PATH.data());
            auto ret = io::FileCacheFactory::instance()->reset_capacity(path, new_capacity);
            LOG(INFO) << ret;
        }
    } else {
        st = Status::InternalError("invalid operation: {}", operation);
    }
    return st;
}

void FileCacheAction::handle(HttpRequest* req) {
    std::string json_metrics;
    Status status = _handle_header(req, &json_metrics);
    std::string status_result = status.to_json();
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, json_metrics);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
    }
}

} // namespace doris
