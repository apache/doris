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

#include <functional>

#include "gen_cpp/PlanNodes_types.h"
#include "http/http_handler.h"
#include "runtime/client_cache.h"
#include "runtime/message_body_sink.h"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;

class StreamLoadWithSqlAction : public HttpHandler {
public:
    StreamLoadWithSqlAction(ExecEnv* exec_env);
    ~StreamLoadWithSqlAction() override;

    void handle(HttpRequest* req) override;

    bool request_will_be_read_progressively() override { return true; }

    int on_header(HttpRequest* req) override;

    void on_chunk_data(HttpRequest* req) override;
    void free_handler_ctx(std::shared_ptr<void> ctx) override;

private:
    Status _on_header(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    Status _handle(HttpRequest* req, std::shared_ptr<StreamLoadContext> ctx);
    Status _data_saved_path(HttpRequest* req, std::string* file_path);
    Status _process_put(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    void _save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx, const std::string& str);
    void _parse_format(const std::string& format_str, const std::string& compress_type_str,
                       TFileFormatType::type* format_type, TFileCompressType::type* compress_type);
    bool _is_format_support_streaming(TFileFormatType::type format);

private:
    ExecEnv* _exec_env;
    std::shared_ptr<MetricEntity> _stream_load_with_sql_entity;
    IntCounter* streaming_load_with_sql_requests_total;
    IntCounter* streaming_load_with_sql_duration_ms;
    IntGauge* streaming_load_with_sql_current_processing;
};

} // namespace doris
