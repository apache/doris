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

#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/compiler_util.h"
#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/proto_util.h"
#include "util/thrift_util.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris {
namespace vectorized {

VWalWriter::VWalWriter(int64_t tb_id, int64_t wal_id, const std::string& import_label,
                       WalManager* wal_manager, std::vector<TSlotDescriptor>& slot_desc,
                       int be_exe_version)
        : _tb_id(tb_id),
          _wal_id(wal_id),
          _label(import_label),
          _wal_manager(wal_manager),
          _slot_descs(slot_desc),
          _be_exe_version(be_exe_version) {}

VWalWriter::~VWalWriter() {}

Status VWalWriter::init() {
    RETURN_IF_ERROR(_wal_manager->create_wal_writer(_wal_id, _wal_writer));
    _wal_manager->add_wal_status_queue(_tb_id, _wal_id, WalManager::WalStatus::CREATE);
    std::stringstream ss;
    for (auto slot_desc : _slot_descs) {
        if (slot_desc.col_unique_id < 0) {
            continue;
        }
        ss << std::to_string(slot_desc.col_unique_id) << ",";
    }
    std::string col_ids = ss.str().substr(0, ss.str().size() - 1);
    RETURN_IF_ERROR(_wal_writer->append_header(_version, col_ids));
    return Status::OK();
}

Status VWalWriter::write_wal(vectorized::Block* block) {
    PBlock pblock;
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    RETURN_IF_ERROR(block->serialize(_be_exe_version, &pblock, &uncompressed_bytes,
                                     &compressed_bytes, segment_v2::CompressionTypePB::SNAPPY));
    RETURN_IF_ERROR(_wal_writer->append_blocks(std::vector<PBlock*> {&pblock}));
    return Status::OK();
}

Status VWalWriter::close() {
    if (_wal_writer != nullptr) {
        RETURN_IF_ERROR(_wal_writer->finalize());
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris