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

#include "common/config.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/storage_engine.h"
#include "util/crc32c.h"

namespace doris {

WalWriter::WalWriter(const std::string& file_name,
                     const std::shared_ptr<std::atomic_size_t>& all_wal_disk_bytes)
        : _file_name(file_name),
          _count(0),
          _disk_bytes(0),
          _all_wal_disk_bytes(all_wal_disk_bytes) {}

WalWriter::~WalWriter() {}

Status WalWriter::init() {
    _batch = config::group_commit_sync_wal_batch;
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
        std::unique_lock l(_mutex);
        while (_all_wal_disk_bytes->load(std::memory_order_relaxed) > config::wal_max_disk_size) {
            cv.wait_for(l, std::chrono::milliseconds(WalWriter::MAX_WAL_WRITE_WAIT_TIME));
        }
    }
    size_t total_size = 0;
    for (const auto& block : blocks) {
        total_size += LENGTH_SIZE + block->ByteSizeLong() + CHECKSUM_SIZE;
    }
    std::string binary(total_size, '\0');
    char* row_binary = binary.data();
    size_t offset = 0;
    for (const auto& block : blocks) {
        unsigned long row_length = block->GetCachedSize();
        memcpy(row_binary + offset, &row_length, LENGTH_SIZE);
        offset += LENGTH_SIZE;
        memcpy(row_binary + offset, block->SerializeAsString().data(), row_length);
        offset += row_length;
        uint32_t checksum = crc32c::Value(block->SerializeAsString().data(), row_length);
        memcpy(row_binary + offset, &checksum, CHECKSUM_SIZE);
        offset += CHECKSUM_SIZE;
    }
    DCHECK(offset == total_size);
    _disk_bytes += total_size;
    _all_wal_disk_bytes->store(
            _all_wal_disk_bytes->fetch_add(total_size, std::memory_order_relaxed),
            std::memory_order_relaxed);
    // write rows
    RETURN_IF_ERROR(_file_writer->append({row_binary, offset}));
    _count++;
    if (_count % _batch == 0) {
        //todo sync data
        //LOG(INFO) << "count=" << count << ",do sync";
    }
    return Status::OK();
}

} // namespace doris
