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

#pragma once

#include <memory>

#include "io/io_common.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"

namespace doris {

inline std::shared_ptr<io::IOContext> create_file_scan_io_context(RuntimeState* state) {
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->query_id = &state->query_id();
    if (state->query_options().query_type == TQueryType::SELECT) {
        io_ctx->reader_type = ReaderType::READER_QUERY;
        if (auto* query_ctx = state->get_query_ctx(); query_ctx != nullptr) {
            io_ctx->remote_scan_cache_write_limiter = query_ctx->remote_scan_cache_write_limiter();
        }
    }
    return io_ctx;
}

} // namespace doris
