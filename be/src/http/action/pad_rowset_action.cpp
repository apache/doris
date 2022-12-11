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

#include "http/action/pad_rowset_action.h"

#include <memory>

#include "http/http_channel.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset.h"
#include "olap/storage_engine.h"

namespace doris {

const std::string TABLET_ID = "tablet_id";
const std::string VERSION = "version";

void PadRowsetAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();
    Status status = _handle(req);
    std::string result = status.to_json();
    LOG(INFO) << "handle request result:" << result;
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, result);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, result);
    }
}

Status PadRowsetAction::_handle(HttpRequest* req) {
    // Get tablet id
    const std::string& tablet_id_str = req->param(TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string("parameter " + TABLET_ID + " not specified in url.");
        return Status::InternalError(error_msg);
    }

    // Get schema hash
    const std::string& version_str = req->param(VERSION);
    if (version_str.empty()) {
        std::string error_msg = std::string("parameter " + VERSION + " not specified in url.");
        return Status::InternalError(error_msg);
    }

    // valid str format
    int64_t tablet_id = std::atol(tablet_id_str.c_str());
    int32_t version = std::atoi(version_str.c_str());

    auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (nullptr == tablet) {
        return Status::InternalError("Unknown tablet id {}", tablet_id);
    }
    return _pad_rowset(tablet, Version(version, version));
}

Status PadRowsetAction::_pad_rowset(TabletSharedPtr tablet, const Version& version) {
    if (tablet->check_version_exist(version)) {
        return Status::InternalError("Input version {} exists", version.to_string());
    }

    std::unique_ptr<RowsetWriter> writer;
    RETURN_IF_ERROR(tablet->create_rowset_writer(version, VISIBLE, NONOVERLAPPING,
                                                 tablet->tablet_schema(), -1, -1, &writer));
    auto rowset = writer->build();
    rowset->make_visible(version);

    std::vector<RowsetSharedPtr> to_add {rowset};
    std::vector<RowsetSharedPtr> to_delete;
    tablet->modify_rowsets(to_add, to_delete);
    tablet->save_meta();

    return Status::OK();
}

} // namespace doris