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

#include "pad_rowset_action.h"

#include <brpc/http_method.h>

#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"

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

PadRowsetHandler::PadRowsetHandler() : BaseHttpHandler("pad_rowset") {}

void PadRowsetHandler::handle_sync(brpc::Controller* cntl) {
    Status status = _handle(cntl);
    std::string result = status.to_json();
    LOG(INFO) << "handle request result:" << result;
    if (status.ok()) {
        on_succ(cntl, result);
    } else {
        on_error(cntl, result);
    }
}

bool PadRowsetHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_POST;
}

Status PadRowsetHandler::_handle(brpc::Controller* cntl) {
    RETURN_IF_ERROR(check_param(cntl));

    const std::string& tablet_id_str = *get_param(cntl, TABLET_ID);
    const std::string& start_version_str = *get_param(cntl, START_VERSION);
    const std::string& end_version_str = *get_param(cntl, END_VERSION);

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

Status PadRowsetHandler::check_param(brpc::Controller* cntl) {
    RETURN_IF_ERROR(check_one_param(*get_param(cntl, TABLET_ID), TABLET_ID));
    RETURN_IF_ERROR(check_one_param(*get_param(cntl, START_VERSION), START_VERSION));
    RETURN_IF_ERROR(check_one_param(*get_param(cntl, END_VERSION), END_VERSION));
    return Status::OK();
}

Status PadRowsetHandler::_pad_rowset(TabletSharedPtr tablet, const Version& version) {
    if (tablet->check_version_exist(version)) {
        return Status::InternalError("Input version {} exists", version.to_string());
    }

    std::unique_ptr<RowsetWriter> writer;
    RowsetWriterContext ctx;
    ctx.version = version;
    ctx.rowset_state = VISIBLE;
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.tablet_schema = tablet->tablet_schema();
    ctx.oldest_write_timestamp = UnixSeconds();
    ctx.newest_write_timestamp = UnixSeconds();
    RETURN_IF_ERROR(tablet->create_rowset_writer(ctx, &writer));
    auto rowset = writer->build();
    rowset->make_visible(version);

    std::vector<RowsetSharedPtr> to_add {rowset};
    std::vector<RowsetSharedPtr> to_delete;
    {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->modify_rowsets(to_add, to_delete);
        tablet->save_meta();
    }

    return Status::OK();
}

} // namespace doris