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

#include <sstream>
#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

Status MetaAction::_handle_header(HttpRequest* req, std::string* json_meta) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_schema_hash = req->param(TABLET_SCHEMA_HASH_KEY);
    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;
    try {
        tablet_id = std::stoull(req_tablet_id);
        schema_hash = std::stoul(req_schema_hash);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id
                     << ", schema_hash:" << req_schema_hash;
        return Status::InternalError(strings::Substitute("convert failed, $0", e.what()));
    }
    BaseTabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_base_tablet(
            tablet_id, schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "no tablet for tablet_id:" << tablet_id << " schema hash:" << schema_hash;
        return Status::InternalError("no tablet exist");
    }
    OLAPStatus s =
            TabletMetaManager::get_json_meta(tablet->data_dir(), tablet_id, schema_hash, json_meta);
    if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
        return Status::InternalError("no header exist");
    } else if (s != OLAP_SUCCESS) {
        return Status::InternalError("backend error");
    }
    return Status::OK();
}

void MetaAction::handle(HttpRequest* req) {
    if (_meta_type == META_TYPE::HEADER) {
        std::string json_meta;
        Status status = _handle_header(req, &json_meta);
        std::string status_result = to_json(status);
        LOG(INFO) << "handle request result:" << status_result;
        if (status.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, json_meta);
        } else {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
        }
    }
}

} // end namespace doris
