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
#include <string>

#include "http/http_handler.h"
#include "util/metrics.h"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;
class HttpRequest;

class StreamLoadAction : public HttpHandler {
public:
    StreamLoadAction(ExecEnv* exec_env);
    ~StreamLoadAction() override;

    void handle(HttpRequest* req) override;

    bool request_will_be_read_progressively() override { return true; }

    int on_header(HttpRequest* req) override;

    void on_chunk_data(HttpRequest* req) override;
    void free_handler_ctx(std::shared_ptr<void> ctx) override;

    // Continue handling after future is ready (called in libevent thread)
    void continue_handle_after_future(std::shared_ptr<StreamLoadContext> ctx,
                                      Status fragment_status, bool need_rollback,
                                      bool need_commit_self, bool body_sink_cancelled);

private:
    Status _on_header(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    Status _handle(std::shared_ptr<StreamLoadContext> ctx, HttpRequest* req);
    Status _data_saved_path(HttpRequest* req, std::string* file_path, int64_t file_bytes);
    Status _process_put(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    void _save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx, const std::string& str);
    Status _handle_group_commit(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);

    // Finalize request and send response
    void _finalize_request(HttpRequest* req, std::shared_ptr<StreamLoadContext> ctx);
    // Cleanup after finalizing request (for statistics and logging)
    void _finalize_request_cleanup(std::shared_ptr<StreamLoadContext> ctx);

    ExecEnv* _exec_env;

    std::shared_ptr<MetricEntity> _stream_load_entity;
    IntCounter* streaming_load_requests_total;
    IntCounter* streaming_load_duration_ms;
    IntGauge* streaming_load_current_processing;
};

} // namespace doris
