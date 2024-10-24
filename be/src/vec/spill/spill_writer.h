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

#include <atomic>
#include <memory>
#include <string>

#include "io/fs/file_writer.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
namespace doris {
class RuntimeState;

namespace vectorized {
class SpillDataDir;
class SpillWriter {
public:
    SpillWriter(int64_t id, size_t batch_size, SpillDataDir* data_dir, const std::string& dir)
            : data_dir_(data_dir), stream_id_(id), batch_size_(batch_size) {
        file_path_ = dir + "/" + std::to_string(file_index_);
    }

    Status open();

    Status close();

    Status write(RuntimeState* state, const Block& block, size_t& written_bytes);

    int64_t get_id() const { return stream_id_; }

    int64_t get_written_bytes() const { return total_written_bytes_; }

    const std::string& get_file_path() const { return file_path_; }

    void set_counters(RuntimeProfile::Counter* serialize_timer,
                      RuntimeProfile::Counter* write_block_counter,
                      RuntimeProfile::Counter* write_bytes_counter,
                      RuntimeProfile::Counter* write_timer) {
        serialize_timer_ = serialize_timer;
        write_block_counter_ = write_block_counter;
        write_bytes_counter_ = write_bytes_counter;
        write_timer_ = write_timer;
    }

private:
    void _init_profile();

    Status _write_internal(const Block& block, size_t& written_bytes);

    // not owned, point to the data dir of this rowset
    // for checking disk capacity when write data to disk.
    SpillDataDir* data_dir_ = nullptr;
    std::atomic_bool closed_ = false;
    int64_t stream_id_;
    size_t batch_size_;
    size_t max_sub_block_size_ = 0;
    int file_index_ = 0;
    std::string file_path_;
    std::unique_ptr<doris::io::FileWriter> file_writer_;

    size_t written_blocks_ = 0;
    int64_t total_written_bytes_ = 0;
    std::string meta_;

    RuntimeProfile::Counter* write_bytes_counter_;
    RuntimeProfile::Counter* serialize_timer_;
    RuntimeProfile::Counter* write_timer_;
    RuntimeProfile::Counter* write_block_counter_;
};
using SpillWriterUPtr = std::unique_ptr<SpillWriter>;
} // namespace vectorized
} // namespace doris
