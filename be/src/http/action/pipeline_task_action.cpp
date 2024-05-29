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

#include "http/action/pipeline_task_action.h"

#include <sstream>
#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

void PipelineTaskAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    HttpChannel::send_reply(req, HttpStatus::OK,
                            ExecEnv::GetInstance()->fragment_mgr()->dump_pipeline_tasks());
}

void LongPipelineTaskAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    int64_t duration = 0;
    try {
        duration = std::stoll(req->param("duration"));
    } catch (const std::exception& e) {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "invalid argument.duration: {}, meet error: {}",
                       req->param("duration"), e.what());
        LOG(WARNING) << fmt::to_string(debug_string_buffer);
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                fmt::to_string(debug_string_buffer));
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::OK,
                            ExecEnv::GetInstance()->fragment_mgr()->dump_pipeline_tasks(duration));
}

void QueryPipelineTaskAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    int64_t high = 0;
    int64_t low = 0;
    try {
        auto& query_id_str = req->param("query_id");
        if (query_id_str.length() != 16 * 2 + 1) {
            HttpChannel::send_reply(
                    req, HttpStatus::INTERNAL_SERVER_ERROR,
                    "Invalid query id! Query id should be {hi}-{lo} which is a hexadecimal. \n");
            return;
        }
        from_hex(&high, query_id_str.substr(0, 16));
        from_hex(&low, query_id_str.substr(17));
    } catch (const std::exception& e) {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "invalid argument.query_id: {}, meet error: {}. \n",
                       req->param("query_id"), e.what());
        LOG(WARNING) << fmt::to_string(debug_string_buffer);
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                fmt::to_string(debug_string_buffer));
        return;
    }
    TUniqueId query_id;
    query_id.hi = high;
    query_id.lo = low;
    HttpChannel::send_reply(req, HttpStatus::OK,
                            ExecEnv::GetInstance()->fragment_mgr()->dump_pipeline_tasks(query_id));
}

} // end namespace doris
