/*
 * @Author: 宋光璠 songguangfan@sf.com
 * @Date: 2024-08-19 10:33:33
 * @LastEditors: 宋光璠 songguangfan@sf.com
 * @LastEditTime: 2024-08-19 16:35:28
 * @FilePath: /doris/be/src/http/action/stream_load.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
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

private:
    Status _on_header(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    Status _handle(std::shared_ptr<StreamLoadContext> ctx);
    Status _data_saved_path(HttpRequest* req, std::string* file_path);
    Status _process_put(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    void _save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx, const std::string& str);
    Status _handle_group_commit(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);

private:
    ExecEnv* _exec_env;

    std::shared_ptr<MetricEntity> _stream_load_entity;
    IntCounter* streaming_load_requests_total;
    IntCounter* streaming_load_duration_ms;
    IntGauge* streaming_load_current_processing;
};

} // namespace doris
