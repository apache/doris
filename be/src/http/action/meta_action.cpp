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

#include "http/action/meta_action.h"

#include <json2pb/pb_to_json.h>
#include <stdint.h>

#include <cstring>
#include <exception>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>

#include "cloud/config.h"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "util/easy_json.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";
const static std::string OP = "op";
const static std::string DATA_SIZE = "data_size";
const static std::string HEADER = "header";

MetaAction::MetaAction(ExecEnv* exec_env, TPrivilegeHier::type hier, TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type) {}

Status MetaAction::_handle_header(HttpRequest* req, std::string* json_meta) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_enable_base64 = req->param(ENABLE_BYTE_TO_BASE64);
    uint64_t tablet_id = 0;
    bool enable_byte_to_base64 = false;
    if (std::strcmp(req_enable_base64.c_str(), "true") == 0) {
        enable_byte_to_base64 = true;
    }
    try {
        tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id
                     << ", enable_byte_to_base64: " << req_enable_base64;
        return Status::InternalError("convert failed, {}", e.what());
    }

    auto tablet = DORIS_TRY(ExecEnv::get_tablet(tablet_id));
    std::string operation = req->param(OP);
    if (operation == HEADER) {
        TabletMeta tablet_meta;
        tablet->generate_tablet_meta_copy(tablet_meta);
        json2pb::Pb2JsonOptions json_options;
        json_options.pretty_json = true;
        json_options.bytes_to_base64 = enable_byte_to_base64;
        tablet_meta.to_json(json_meta, json_options);
        return Status::OK();
    } else if (operation == DATA_SIZE) {
        if (!config::is_cloud_mode()) {
            EasyJson data_size;
            {
                auto* local_tablet = static_cast<Tablet*>(tablet.get());
                std::shared_lock rowset_ldlock(tablet->get_header_lock());
                data_size["local_data_size"] = local_tablet->tablet_local_size();
                data_size["remote_data_size"] = local_tablet->tablet_remote_size();
            }
            *json_meta = data_size.ToString();
        }
        return Status::OK();
    }
    return Status::InternalError("invalid operation");
}

void MetaAction::handle(HttpRequest* req) {
    std::string json_meta;
    Status status = _handle_header(req, &json_meta);
    std::string status_result = status.to_json();
    LOG(INFO) << "handle request result:" << status_result;
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, json_meta);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
    }
}

} // end namespace doris
