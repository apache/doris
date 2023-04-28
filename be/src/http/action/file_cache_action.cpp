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

#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta.h"
#include "util/easy_json.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";
const static std::string OP = "op";

Status FileCacheAction::_handle_header(HttpRequest* req, std::string* json_metrics) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string operation = req->param(OP);
    if (operation == "metrics") {
        auto statistics = io::FileCacheFactory::instance().get_cache_statistics();
        std::ostringstream result;
        result << "[";
        for (int i = 0; i < statistics.size(); ++i) {
            auto& metrics = statistics[i];
            EasyJson json;
            json["cache_path"] = metrics.cache_path;
            json["hits_ratio"] = metrics.hits_ratio;
            json["removed_elements"] = metrics.removed_elements;

            json["index_queue_max_size"] = metrics.index_queue_max_size;
            json["index_queue_curr_size"] = metrics.index_queue_curr_size;
            json["index_queue_max_elements"] = metrics.index_queue_max_elements;
            json["index_queue_curr_elements"] = metrics.index_queue_curr_elements;

            json["normal_queue_max_size"] = metrics.normal_queue_max_size;
            json["normal_queue_curr_size"] = metrics.normal_queue_curr_size;
            json["normal_queue_max_elements"] = metrics.normal_queue_max_elements;
            json["normal_queue_curr_elements"] = metrics.normal_queue_curr_elements;

            json["disposable_queue_max_size"] = metrics.disposable_queue_max_size;
            json["disposable_queue_curr_size"] = metrics.disposable_queue_curr_size;
            json["disposable_queue_max_elements"] = metrics.disposable_queue_max_elements;
            json["disposable_queue_curr_elements"] = metrics.disposable_queue_curr_elements;

            if (i != 0) {
                result << ", ";
            }
            result << json.ToString();
        }
        result << "]";
        *json_metrics = result.str();
        return Status::OK();
    } else if (operation == "release") {
        size_t released = 0;
        if (req->param("base_path") != "") {
            released = io::FileCacheFactory::instance().try_release(req->param("base_path"));
        } else {
            released = io::FileCacheFactory::instance().try_release();
        }
        EasyJson json;
        json["released_elements"] = released;
        *json_metrics = json.ToString();
        return Status::OK();
    }
    return Status::InternalError("invalid operation: {}", operation);
}

void FileCacheAction::handle(HttpRequest* req) {
    std::string json_metrics;
    Status status = _handle_header(req, &json_metrics);
    std::string status_result = status.to_json();
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, json_metrics);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
    }
}

} // namespace doris
