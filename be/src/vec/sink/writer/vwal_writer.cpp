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

#include "vwal_writer.h"

#include <gen_cpp/data.pb.h>

#include <sstream>

#include "util/debug_points.h"

namespace doris {
namespace vectorized {

VWalWriter::VWalWriter(int64_t db_id, int64_t tb_id, int64_t wal_id,
                       const std::string& import_label, WalManager* wal_manager,
                       std::vector<TSlotDescriptor>& slot_desc, int be_exe_version)
        : _db_id(db_id),
          _tb_id(tb_id),
          _wal_id(wal_id),
          _label(import_label),
          _wal_manager(wal_manager),
          _slot_descs(slot_desc),
          _be_exe_version(be_exe_version) {}

VWalWriter::~VWalWriter() {}

Status VWalWriter::init() {
#ifndef BE_TEST
    if (config::group_commit_wait_replay_wal_finish) {
        std::shared_ptr<std::mutex> lock = std::make_shared<std::mutex>();
        std::shared_ptr<std::condition_variable> cv = std::make_shared<std::condition_variable>();
        auto add_st = _wal_manager->add_wal_cv_map(_wal_id, lock, cv);
        if (!add_st.ok()) {
            LOG(WARNING) << "fail to add wal_id " << _wal_id << " to wal_cv_map";
        }
    }
#endif
    RETURN_IF_ERROR(_create_wal_writer(_wal_id, _wal_writer));
    _wal_manager->add_wal_queue(_tb_id, _wal_id);
    std::stringstream ss;
    for (auto slot_desc : _slot_descs) {
        if (slot_desc.col_unique_id < 0) {
            continue;
        }
        ss << std::to_string(slot_desc.col_unique_id) << ",";
    }
    std::string col_ids = ss.str().substr(0, ss.str().size() - 1);
    RETURN_IF_ERROR(_wal_writer->append_header(col_ids));
    return Status::OK();
}

Status VWalWriter::write_wal(vectorized::Block* block) {
    DBUG_EXECUTE_IF("VWalWriter.write_wal.fail",
                    { return Status::InternalError("Failed to write wal!"); });
    PBlock pblock;
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    RETURN_IF_ERROR(block->serialize(_be_exe_version, &pblock, &uncompressed_bytes,
                                     &compressed_bytes,
                                     segment_v2::CompressionTypePB::NO_COMPRESSION));
    RETURN_IF_ERROR(_wal_writer->append_blocks(std::vector<PBlock*> {&pblock}));
    return Status::OK();
}

Status VWalWriter::close() {
    if (config::group_commit_wait_replay_wal_finish) {
        std::string wal_path;
        RETURN_IF_ERROR(_wal_manager->get_wal_path(_wal_id, wal_path));
        LOG(INFO) << "close file " << wal_path;
        RETURN_IF_ERROR(_wal_manager->add_recover_wal(_db_id, _tb_id, _wal_id, wal_path));
        RETURN_IF_ERROR(_wal_manager->wait_replay_wal_finish(_wal_id));
    }
    if (_wal_writer != nullptr) {
        RETURN_IF_ERROR(_wal_writer->finalize());
    }
    return Status::OK();
}

Status VWalWriter::_create_wal_writer(int64_t wal_id, std::shared_ptr<WalWriter>& wal_writer) {
    std::string wal_path;
    RETURN_IF_ERROR(_wal_manager->get_wal_path(wal_id, wal_path));
    wal_writer = std::make_shared<WalWriter>(wal_path);
    RETURN_IF_ERROR(wal_writer->init());
    return Status::OK();
}
} // namespace vectorized
} // namespace doris