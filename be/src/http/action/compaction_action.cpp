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

#include "http/action/compaction_action.h"

#include <sstream>
#include <string>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_headers.h"
#include "http/http_status.h"

#include "olap/storage_engine.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

// for viewing the compaction status
Status CompactionAction::_handle_show_compaction(HttpRequest *req, std::string* json_result) {
    std::string req_tablet_id = req->param(TABLET_ID_KEY);
    std::string req_schema_hash = req->param(TABLET_SCHEMA_HASH_KEY);
    if (req_tablet_id == "" && req_schema_hash == "") {
        // TODO(cmy): View the overall compaction status
        return Status::NotSupported("The overall compaction status is not supported yet");
    } else if (req_tablet_id == "" || req_schema_hash == "") {
        return Status::InvalidArgument("Missing tablet id or schema hash");
    }

    uint64_t tablet_id = std::stoull(req_tablet_id);
    uint32_t schema_hash = std::stoul(req_schema_hash);

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found");
    }

    OLAPStatus s = tablet->get_compaction_status(json_result);
    if (s != OLAP_SUCCESS) {
        return Status::InternalError(strings::Substitute("failed to get tablet compaction status. res $0", s));
    }
    return Status::OK();
}

void CompactionAction::handle(HttpRequest *req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());

    if (_type == CompactionActionType::SHOW_INFO) {
        std::string json_result;
        Status st = _handle_show_compaction(req, &json_result);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::OK, to_json(st));
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK, json_result);
        }
    } else {
        HttpChannel::send_reply(req, HttpStatus::OK, to_json(Status::NotSupported("Action not supported")));
    }
}

} // end namespace doris
