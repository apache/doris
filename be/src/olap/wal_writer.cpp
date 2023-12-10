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

#include "olap/wal_writer.h"

#include <atomic>
#include <condition_variable>
#include <memory>

#include "common/config.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/storage_engine.h"
#include "util/crc32c.h"

namespace doris {

const char* k_wal_magic = "WAL1";
const uint32_t k_wal_magic_length = 4;

WalWriter::WalWriter(const std::string& file_name,
                     const std::shared_ptr<std::atomic_size_t>& all_wal_disk_bytes,
                     const std::shared_ptr<std::condition_variable>& cv)
        : cv(cv),
          _file_name(file_name),
          _disk_bytes(0),
          _all_wal_disk_bytes(all_wal_disk_bytes),
          _is_first_append_blocks(true) {}

WalWriter::~WalWriter() {}

Status WalWriter::init() {
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(_file_name, &_file_writer));
    return Status::OK();
}

Status WalWriter::finalize() {
    auto st = _file_writer->close();
    if (!st.ok()) {
        LOG(WARNING) << "fail to close file " << _file_name;
    }
    return Status::OK();
}

Status WalWriter::append_blocks(const PBlockArray& blocks) {
    {
        if (_is_first_append_blocks) {
            _is_first_append_blocks = false;
            std::unique_lock l(_mutex);
            while (_all_wal_disk_bytes->load(std::memory_order_relaxed) >
                   config::wal_max_disk_size) {
                LOG(INFO) << "First time to append blocks to wal file " << _file_name
                          << ". Currently, all wal disk space usage is "
                          << _all_wal_disk_bytes->load(std::memory_order_relaxed)
                          << ", larger than the maximum limit " << config::wal_max_disk_size
                          << ", so we need to wait. When any other load finished, that wal will be "
                             "removed, the space used by that wal will be free.";
                cv->wait_for(l, std::chrono::milliseconds(WalWriter::MAX_WAL_WRITE_WAIT_TIME));
            }
        }
    }
    size_t total_size = 0;
    for (const auto& block : blocks) {
        total_size += LENGTH_SIZE + block->ByteSizeLong() + CHECKSUM_SIZE;
    }
    size_t offset = 0;
    for (const auto& block : blocks) {
        uint8_t len_buf[sizeof(uint64_t)];
        uint64_t block_length = block->ByteSizeLong();
        encode_fixed64_le(len_buf, block_length);
        RETURN_IF_ERROR(_file_writer->append({len_buf, sizeof(uint64_t)}));
        offset += LENGTH_SIZE;
        std::string content = block->SerializeAsString();
        RETURN_IF_ERROR(_file_writer->append(content));
        offset += block_length;
        uint8_t checksum_buf[sizeof(uint32_t)];
        uint32_t checksum = crc32c::Value(content.data(), block_length);
        encode_fixed32_le(checksum_buf, checksum);
        RETURN_IF_ERROR(_file_writer->append({checksum_buf, sizeof(uint32_t)}));
        offset += CHECKSUM_SIZE;
    }
    if (offset != total_size) {
        return Status::InternalError(
                "failed to write block to wal expected= " + std::to_string(total_size) +
                ",actually=" + std::to_string(offset));
    }
    _disk_bytes.fetch_add(total_size, std::memory_order_relaxed);
    _all_wal_disk_bytes->fetch_add(total_size, std::memory_order_relaxed);
    return Status::OK();
}

Status WalWriter::append_header(uint32_t version, std::string col_ids) {
    size_t total_size = 0;
    uint64_t length = col_ids.size();
    total_size += k_wal_magic_length;
    total_size += VERSION_SIZE;
    total_size += LENGTH_SIZE;
    total_size += length;
    size_t offset = 0;
    RETURN_IF_ERROR(_file_writer->append({k_wal_magic, k_wal_magic_length}));
    offset += k_wal_magic_length;

    uint8_t version_buf[sizeof(uint32_t)];
    encode_fixed32_le(version_buf, version);
    RETURN_IF_ERROR(_file_writer->append({version_buf, sizeof(uint32_t)}));
    offset += VERSION_SIZE;
    uint8_t len_buf[sizeof(uint64_t)];
    encode_fixed64_le(len_buf, length);
    RETURN_IF_ERROR(_file_writer->append({len_buf, sizeof(uint64_t)}));
    offset += LENGTH_SIZE;
    RETURN_IF_ERROR(_file_writer->append(col_ids));
    offset += length;
    if (offset != total_size) {
        return Status::InternalError(
                "failed to write header to wal expected= " + std::to_string(total_size) +
                ",actually=" + std::to_string(offset));
    }
    return Status::OK();
}

} // namespace doris
