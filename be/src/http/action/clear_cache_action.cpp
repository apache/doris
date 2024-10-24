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

#include "http/action/clear_cache_action.h"

#include <sstream>
#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/memory/cache_manager.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

void ClearCacheAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    std::string cache_type_str = req->param("type");
    fmt::memory_buffer return_string_buffer;
    int64_t freed_size = 0;
    if (cache_type_str == "all") {
        freed_size = CacheManager::instance()->for_each_cache_prune_all(nullptr, true);
    } else {
        CachePolicy::CacheType cache_type = CachePolicy::string_to_type(cache_type_str);
        if (cache_type == CachePolicy::CacheType::NONE) {
            fmt::format_to(return_string_buffer,
                           "ClearCacheAction not match type:{} of cache policy", cache_type_str);
            LOG(WARNING) << fmt::to_string(return_string_buffer);
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    fmt::to_string(return_string_buffer));
            return;
        }
        freed_size = CacheManager::instance()->cache_prune_all(cache_type, true);
        if (freed_size == -1) {
            fmt::format_to(return_string_buffer,
                           "ClearCacheAction cache:{} is not allowed to be pruned", cache_type_str);
            LOG(WARNING) << fmt::to_string(return_string_buffer);
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    fmt::to_string(return_string_buffer));
            return;
        }
    }
    fmt::format_to(return_string_buffer, "ClearCacheAction cache:{} prune win, freed size {}",
                   cache_type_str, freed_size);
    LOG(WARNING) << fmt::to_string(return_string_buffer);
    HttpChannel::send_reply(req, HttpStatus::OK, fmt::to_string(return_string_buffer));
}

} // end namespace doris
