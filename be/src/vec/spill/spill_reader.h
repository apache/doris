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

#include <gen_cpp/data.pb.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {
class Block;
class SpillReader {
public:
    SpillReader(int64_t stream_id, std::string file_path)
            : stream_id_(stream_id), file_path_(std::move(file_path)) {}

    ~SpillReader() { (void)close(); }

    Status open();

    Status close();

    Status read(Block* block, bool* eos);

    void seek(size_t block_index);

    int64_t get_id() const { return stream_id_; }

    std::string get_path() const { return file_path_; }

    size_t block_count() const { return block_count_; }

    void set_counters(RuntimeProfile::Counter* read_timer,
                      RuntimeProfile::Counter* deserialize_timer,
                      RuntimeProfile::Counter* read_bytes) {
        read_timer_ = read_timer;
        deserialize_timer_ = deserialize_timer;
        read_bytes_ = read_bytes;
    }

private:
    int64_t stream_id_;
    std::string file_path_;
    io::FileReaderSPtr file_reader_;

    size_t block_count_ = 0;
    size_t read_block_index_ = 0;
    size_t max_sub_block_size_ = 0;
    std::unique_ptr<char[]> read_buff_;
    std::vector<size_t> block_start_offsets_;

    PBlock pb_block_;

    RuntimeProfile::Counter* read_timer_;
    RuntimeProfile::Counter* deserialize_timer_;
    RuntimeProfile::Counter* read_bytes_;
};

using SpillReaderUPtr = std::unique_ptr<SpillReader>;

} // namespace doris::vectorized