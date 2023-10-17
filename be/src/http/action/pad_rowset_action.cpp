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

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <cstdlib>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {

const std::string TABLET_ID = "tablet_id";
const std::string START_VERSION = "start_version";
const std::string END_VERSION = "end_version";

Status check_one_param(const std::string& param_val, const std::string& param_name) {
    if (param_val.empty()) {
        return Status::InternalError("paramater {} not specified in url", param_name);
    }
    return Status::OK();
}

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

Status PadRowsetAction::check_param(HttpRequest* req) {
    RETURN_IF_ERROR(check_one_param(req->param(TABLET_ID), TABLET_ID));
    RETURN_IF_ERROR(check_one_param(req->param(START_VERSION), START_VERSION));
    RETURN_IF_ERROR(check_one_param(req->param(END_VERSION), END_VERSION));
    return Status::OK();
}

Status PadRowsetAction::_handle(HttpRequest* req) {
    RETURN_IF_ERROR(check_param(req));

    const std::string& tablet_id_str = req->param(TABLET_ID);
    const std::string& start_version_str = req->param(START_VERSION);
    const std::string& end_version_str = req->param(END_VERSION);

    // valid str format
    int64_t tablet_id = std::atol(tablet_id_str.c_str());
    int32_t start_version = std::atoi(start_version_str.c_str());
    int32_t end_version = std::atoi(end_version_str.c_str());
    if (start_version < 0 || end_version < 0 || end_version < start_version) {
        return Status::InternalError("Invalid input version");
    }

    auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (nullptr == tablet) {
        return Status::InternalError("Unknown tablet id {}", tablet_id);
    }
    return _pad_rowset(tablet, Version(start_version, end_version));
}

Status PadRowsetAction::_pad_rowset(TabletSharedPtr tablet, const Version& version) {
    if (tablet->check_version_exist(version)) {
        return Status::InternalError("Input version {} exists", version.to_string());
    }

    std::unique_ptr<RowsetWriter> writer;
    RowsetWriterContext ctx;
    ctx.version = version;
    ctx.rowset_state = VISIBLE;
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.tablet_schema = tablet->tablet_schema();
    ctx.newest_write_timestamp = UnixSeconds();
    RETURN_IF_ERROR(tablet->create_rowset_writer(ctx, &writer));
    RowsetSharedPtr rowset;
    RETURN_IF_ERROR(writer->build(rowset));
    rowset->make_visible(version);

    std::vector<RowsetSharedPtr> to_add {rowset};
    std::vector<RowsetSharedPtr> to_delete;
    {
        std::unique_lock wlock(tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        static_cast<void>(tablet->modify_rowsets(to_add, to_delete));
        tablet->save_meta();
    }

    return Status::OK();
}

} // namespace doris
