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

#ifndef DORIS_BE_SRC_COMMON_UTIL_MINI_LOAD_H
#define DORIS_BE_SRC_COMMON_UTIL_MINI_LOAD_H

#include <map>
#include <mutex>
#include <set>
#include <string>

#include "common/status.h"
#include "gen_cpp/FrontendService.h"
#include "http/http_handler.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/defer_op.h"

namespace doris {

// Used to identify one mini load job
struct LoadHandle {
    std::string db;
    std::string label;
    std::string sub_label;
};

struct LoadHandleCmp {
    bool operator()(const LoadHandle& lhs, const LoadHandle& rhs) const;
};

class TMasterResult;
class ExecEnv;
class StreamLoadContext;

// This a handler for mini load
// path is /api/{db}/{table}/_load
class MiniLoadAction : public HttpHandler {
public:
    MiniLoadAction(ExecEnv* exec_env);

    virtual ~MiniLoadAction() {}

    void handle(HttpRequest* req) override;

    bool request_will_be_read_progressively() override { return true; }

    int on_header(HttpRequest* req) override;

    void on_chunk_data(HttpRequest* req) override;
    void free_handler_ctx(void* ctx) override;

    void erase_handle(const LoadHandle& handle);

private:
    Status _load(HttpRequest* req, const std::string& file_path, const std::string& user,
                 const std::string& cluster, int64_t file_size);

    Status data_saved_dir(const LoadHandle& desc, const std::string& table, std::string* file_path);

    Status _on_header(HttpRequest* http_req);

    Status generate_check_load_req(const HttpRequest* http_req, TLoadCheckRequest* load_check_req);

    Status check_auth(const HttpRequest* http_req, const TLoadCheckRequest& load_check_req);

    void _on_chunk_data(HttpRequest* http_req);

    void _handle(HttpRequest* http_req);

    // streaming mini load
    Status _on_new_header(HttpRequest* req);

    Status _begin_mini_load(StreamLoadContext* ctx);

    Status _process_put(HttpRequest* req, StreamLoadContext* ctx);

    void _on_new_chunk_data(HttpRequest* http_req);

    void _new_handle(HttpRequest* req);

    Status _on_new_handle(StreamLoadContext* ctx);

    bool _is_streaming(HttpRequest* req);

    Status _merge_header(HttpRequest* http_req, std::map<std::string, std::string>* params);

    const std::string _streaming_function_name = "STREAMING_MINI_LOAD";

    ExecEnv* _exec_env;

    std::mutex _lock;
    // Used to check if load is duplicated in this instance.
    std::set<LoadHandle, LoadHandleCmp> _current_load;
};

} // namespace doris
#endif
