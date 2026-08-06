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

#pragma once

#include <fstream>

#include "common/status.h"

namespace doris {
template <typename PB>
Status read_pb(const std::string& pb_filename, PB* pb) {
    std::fstream pb_file_stream(pb_filename, std::ios::in | std::ios::binary);
    if (pb_file_stream.bad()) {
        auto err_msg = fmt::format("fail to open rowset binlog metas file. [path={}]", pb_filename);
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    if (pb->ParseFromIstream(&pb_file_stream)) {
        return Status::OK();
    }
    return Status::InternalError("fail to parse rowset binlog metas file");
}

template <typename PB>
Status write_pb(const std::string& pb_filename, const PB& pb) {
    std::fstream pb_file_stream(pb_filename, std::ios::out | std::ios::trunc | std::ios::binary);
    if (pb_file_stream.bad()) {
        auto err_msg = fmt::format("fail to open rowset binlog metas file. [path={}]", pb_filename);
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    if (!pb.SerializeToOstream(&pb_file_stream)) {
        auto err_msg =
                fmt::format("fail to save rowset binlog metas to file. [path={}]", pb_filename);
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    if (!pb_file_stream.flush()) {
        auto err_msg =
                fmt::format("fail to flush rowset binlog metas to file. [path={}]", pb_filename);
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    pb_file_stream.close();
    if (pb_file_stream.bad()) {
        auto err_msg =
                fmt::format("fail to close rowset binlog metas file. [path={}]", pb_filename);
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    return Status::OK();
}
} // namespace doris
