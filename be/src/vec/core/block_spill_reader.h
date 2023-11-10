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
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/runtime_profile.h"

namespace doris {
namespace vectorized {
class Block;

// Read data spilled to local file.
class BlockSpillReader {
public:
    BlockSpillReader(int64_t stream_id, const std::string& file_path, RuntimeProfile* profile,
                     bool delete_after_read = true)
            : stream_id_(stream_id),
              file_path_(file_path),
              delete_after_read_(delete_after_read),
              profile_(profile) {
        _init_profile();
    }

    ~BlockSpillReader() { static_cast<void>(close()); }

    Status open();

    Status close();

    Status read(Block* block, bool* eos);

    void seek(size_t block_index);

    int64_t get_id() const { return stream_id_; }

    std::string get_path() const { return file_path_; }

    size_t block_count() const { return block_count_; }

private:
    void _init_profile();

    int64_t stream_id_;
    std::string file_path_;
    bool delete_after_read_;
    io::FileReaderSPtr file_reader_;

    size_t block_count_ = 0;
    size_t read_block_index_ = 0;
    size_t max_sub_block_size_ = 0;
    std::unique_ptr<char[]> read_buff_;
    std::vector<size_t> block_start_offsets_;

    RuntimeProfile* profile_ = nullptr;
    RuntimeProfile::Counter* read_time_;
    RuntimeProfile::Counter* deserialize_time_;
    RuntimeProfile::Counter* read_bytes_;
    RuntimeProfile::Counter* read_block_num_;
};

using BlockSpillReaderUPtr = std::unique_ptr<BlockSpillReader>;
} // namespace vectorized
} // namespace doris
