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

#include "meta_action.h"

#include <brpc/http_method.h>

#include <string>

#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "util/json_util.h"

namespace doris {

MetaHandler::MetaHandler(META_TYPE meta_type)
        : doris::BaseHttpHandler("meta"), _meta_type(meta_type) {}

void MetaHandler::handle_sync(brpc::Controller* cntl) {
    if (_meta_type == META_TYPE::HEADER) {
        std::string json_meta;
        Status status = _handle_header(cntl, &json_meta);
        std::string status_result = status.to_json();
        LOG(INFO) << "handle request result:" << status_result;
        if (status.ok()) {
            on_succ_json(cntl, json_meta);
        } else {
            on_error(cntl, status_result);
        }
    }
}

bool MetaHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET;
}

Status MetaHandler::_handle_header(brpc::Controller* cntl, std::string* json_meta) {
    const std::string& req_tablet_id = cntl->http_request().unresolved_path();
    const std::string* req_enable_base64_ptr = get_param(cntl, ENABLE_BYTE_TO_BASE64);
    std::string req_enable_base64 = req_enable_base64_ptr == nullptr ? "" : *req_enable_base64_ptr;
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

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "no tablet for tablet_id:" << tablet_id;
        return Status::InternalError("no tablet exist");
    }
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    tablet->generate_tablet_meta_copy(tablet_meta);
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    json_options.bytes_to_base64 = enable_byte_to_base64;
    tablet_meta->to_json(json_meta, json_options);
    return Status::OK();
}

} // namespace doris