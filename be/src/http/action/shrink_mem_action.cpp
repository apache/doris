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

#include "http/action/shrink_mem_action.h"

#include <fmt/core.h>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "runtime/exec_env.h"
#include "runtime/memory/memory_reclamation.h"
#include "util/brpc_client_cache.h"
#include "util/mem_info.h"
#include "util/string_util.h"

namespace doris {
void ShrinkMemAction::handle(HttpRequest* req) {
    LOG(INFO) << "begin shrink memory";
    /* this interface might be ready for cloud in the near future
     * int freed_mem = 0;
     * doris::MemInfo::process_cache_gc(&freed_mem); */
    MemoryReclamation::process_minor_gc();
    LOG(INFO) << "shrink memory triggered, using Process Minor GC Free Memory";
    HttpChannel::send_reply(req, HttpStatus::OK, "shrinking");
}

} // namespace doris
