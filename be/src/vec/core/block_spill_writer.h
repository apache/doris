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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
namespace vectorized {

// Write a block to a local file.
//
// The block may be logically splitted to small sub blocks in the single file,
// which can be read back one small block at a time by BlockSpillReader::read.
//
// Split to small blocks is necessary for Sort node, which need to merge multiple
// spilled big sorted blocks into a bigger sorted block. A small block is read from each
// spilled block file each time.
class BlockSpillWriter {
public:
    BlockSpillWriter(int64_t id, size_t batch_size, const std::string& file_path,
                     RuntimeProfile* profile)
            : stream_id_(id), batch_size_(batch_size), file_path_(file_path), profile_(profile) {
        _init_profile();
    }

    ~BlockSpillWriter() { static_cast<void>(close()); }

    Status open();

    Status close();

    Status write(const Block& block);

    int64_t get_id() const { return stream_id_; }

    size_t get_written_bytes() const { return total_written_bytes_; }

private:
    void _init_profile();

    Status _write_internal(const Block& block);

private:
    bool is_open_ = false;
    int64_t stream_id_;
    size_t batch_size_;
    size_t max_sub_block_size_ = 0;
    std::string file_path_;
    std::unique_ptr<doris::io::FileWriter> file_writer_;

    size_t written_blocks_ = 0;
    size_t total_written_bytes_ = 0;
    std::string meta_;

    bool is_first_write_ = true;
    Block tmp_block_;

    RuntimeProfile* profile_ = nullptr;
    RuntimeProfile::Counter* write_bytes_counter_;
    RuntimeProfile::Counter* serialize_timer_;
    RuntimeProfile::Counter* write_timer_;
    RuntimeProfile::Counter* write_blocks_num_;
};

using BlockSpillWriterUPtr = std::unique_ptr<BlockSpillWriter>;
} // namespace vectorized
} // namespace doris
