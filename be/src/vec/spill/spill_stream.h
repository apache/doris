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
#include <future>
#include <memory>

#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeProfile;
class ThreadPool;

namespace vectorized {

class Block;
class SpillDataDir;

class SpillStream {
public:
    // to avoid too many small file writes
    static constexpr size_t MIN_SPILL_WRITE_BATCH_MEM = 32 * 1024;
    static constexpr size_t MAX_SPILL_WRITE_BATCH_MEM = 32 * 1024 * 1024;
    SpillStream(RuntimeState* state, int64_t stream_id, SpillDataDir* data_dir,
                std::string spill_dir, size_t batch_rows, size_t batch_bytes,
                RuntimeProfile* profile);

    SpillStream() = delete;

    ~SpillStream();

    void gc();

    int64_t id() const { return stream_id_; }

    SpillDataDir* get_data_dir() const { return data_dir_; }
    const std::string& get_spill_root_dir() const;

    const std::string& get_spill_dir() const { return spill_dir_; }

    int64_t get_written_bytes() const { return total_written_bytes_; }

    Status spill_block(RuntimeState* state, const Block& block, bool eof);

    Status spill_eof();

    Status read_next_block_sync(Block* block, bool* eos);

    void set_read_counters(RuntimeProfile* operator_profile) {
        reader_->set_counters(operator_profile);
    }

    void update_shared_profiles(RuntimeProfile* source_op_profile);

    SpillReaderUPtr create_separate_reader() const;

    const TUniqueId& query_id() const;

    bool ready_for_reading() const { return _ready_for_reading; }

private:
    friend class SpillStreamManager;

    Status prepare();

    void _set_write_counters(RuntimeProfile* profile) { writer_->set_counters(profile); }

    RuntimeState* state_ = nullptr;
    int64_t stream_id_;
    SpillDataDir* data_dir_ = nullptr;
    // Directory path format specified in SpillStreamManager::register_spill_stream:
    // storage_root/spill/query_id/partitioned_hash_join-node_id-task_id-stream_id
    std::string spill_dir_;
    size_t batch_rows_;
    size_t batch_bytes_;
    int64_t total_written_bytes_ = 0;

    std::atomic_bool _ready_for_reading = false;
    std::atomic_bool _is_reading = false;

    SpillWriterUPtr writer_;
    SpillReaderUPtr reader_;

    TUniqueId query_id_;

    RuntimeProfile* profile_ = nullptr;
    RuntimeProfile::Counter* _current_file_count = nullptr;
    RuntimeProfile::Counter* _total_file_count = nullptr;
    RuntimeProfile::Counter* _current_file_size = nullptr;
};
using SpillStreamSPtr = std::shared_ptr<SpillStream>;
} // namespace vectorized
} // namespace doris
#include "common/compile_check_end.h"
