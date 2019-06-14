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
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_headers.h"
#include "http/http_status.h"

#include "olap/olap_header_manager.h"
#include "olap/olap_engine.h"
#include "olap/olap_define.h"
#include "olap/olap_header.h"
#include "olap/olap_table.h"
#include "common/logging.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

Status MetaAction::_handle_header(HttpRequest *req, std::string* json_header) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_schema_hash = req->param(TABLET_SCHEMA_HASH_KEY);
    if (req_tablet_id == "" || req_schema_hash == "") {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id
                << ", schema_hash:" << req_schema_hash;
        return Status::InternalError("invalid arguments");
    }
    uint64_t tablet_id = std::stoull(req_tablet_id);
    uint32_t schema_hash = std::stoul(req_schema_hash);
    OLAPTablePtr olap_table = OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    if (olap_table == nullptr) {
        LOG(WARNING) << "no tablet for tablet_id:" << tablet_id << " schema hash:" << schema_hash;
        return Status::InternalError("no tablet exist");
    }
    OLAPStatus s = OlapHeaderManager::get_json_header(olap_table->store(), tablet_id, schema_hash, json_header);
    if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
        return Status::InternalError("no header exist");
    } else if (s != OLAP_SUCCESS) {
        return Status::InternalError("backend error");
    }
    return Status::OK();
}

void MetaAction::handle(HttpRequest *req) {
    if (_meta_type == META_TYPE::HEADER) {
        std::string json_header;
        Status status = _handle_header(req, &json_header);
        std::string status_result = to_json(status);
        LOG(INFO) << "handle request result:" << status_result;
        if (status.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, json_header);
        } else {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
        }
    }
}

} // end namespace doris
