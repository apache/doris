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

#include "http/action/clear_file_cache_action.h"

#include <fmt/core.h>

#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "io/cache/block_file_cache_factory.h"

namespace doris {

const std::string SYNC = "sync";

void ClearFileCacheAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    std::string sync = req->param(SYNC);
    auto ret =
            io::FileCacheFactory::instance()->clear_file_caches(sync == "TRUE" || sync == "true");
    HttpChannel::send_reply(req, HttpStatus::OK, ret);
}

} // namespace doris
