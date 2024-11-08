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

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
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
constexpr static std::string_view HASH = "hash";
constexpr static std::string_view LIST_CACHE = "list_cache";
constexpr static std::string_view CAPACITY = "capacity";
constexpr static std::string_view RELEASE = "release";
constexpr static std::string_view BASE_PATH = "base_path";
constexpr static std::string_view RELEASED_ELEMENTS = "released_elements";
constexpr static std::string_view VALUE = "value";

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
        const std::string& segment_path = req->param(VALUE.data());
        if (segment_path.empty()) {
            io::FileCacheFactory::instance()->clear_file_caches(to_lower(sync) == "true");
        } else {
            io::UInt128Wrapper hash = io::BlockFileCache::hash(segment_path);
            io::BlockFileCache* cache = io::FileCacheFactory::instance()->get_by_path(hash);
            cache->remove_if_cached(hash);
        }
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
    } else if (operation == HASH) {
        const std::string& segment_path = req->param(VALUE.data());
        if (segment_path.empty()) {
            st = Status::InvalidArgument("missing parameter: {} is required", VALUE.data());
        } else {
            io::UInt128Wrapper ret = io::BlockFileCache::hash(segment_path);
            EasyJson json;
            json[HASH.data()] = ret.to_string();
            *json_metrics = json.ToString();
        }
    } else if (operation == LIST_CACHE) {
        const std::string& segment_path = req->param(VALUE.data());
        if (segment_path.empty()) {
            st = Status::InvalidArgument("missing parameter: {} is required", VALUE.data());
        } else {
            io::UInt128Wrapper cache_hash = io::BlockFileCache::hash(segment_path);
            std::vector<std::string> cache_files =
                    io::FileCacheFactory::instance()->get_cache_file_by_path(cache_hash);
            if (cache_files.empty()) {
                *json_metrics = "[]";
            } else {
                EasyJson json;
                std::for_each(cache_files.begin(), cache_files.end(),
                              [&json](auto& x) { json.PushBack(x); });
                *json_metrics = json.ToString();
            }
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
        HttpChannel::send_reply(req, HttpStatus::OK,
                                json_metrics.empty() ? status.to_json() : json_metrics);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
    }
}

} // namespace doris
